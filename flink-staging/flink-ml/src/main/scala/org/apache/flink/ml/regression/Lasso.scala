/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.regression

import breeze.linalg._
import breeze.numerics.{abs, signum}
import org.apache.flink.api.common.functions.{Partitioner, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import com.github.fommil.netlib.BLAS.{ getInstance => blas }

/**
 * Created by Thomas Peel @ Eura Nova
 * on 18/05/15.
 */

class Lasso(
  beta: Double,
  numIter: Int,
  normalize: Boolean = false,
  line_search: Boolean = false,
  epsilon: Double = 1e-3,
  opt: String = "GR")
  extends Serializable {

  def fit(
    data: DataSet[ColumnVector], target: DataSet[Array[Double]]): DataSet[LassoModel] = {
    val Y = if (normalize) {
      target map { x => breeze.linalg.normalize(DenseVector(x)).toArray }
    } else {
      target
    }

    val initialSolution = Y map {
      y => PartialLassoSolution(y, SparseApproximation.initialApproximation, -1.0)
    }

    val iteration: DataSet[PartialLassoSolution] = opt match {
      case "CD" => {
        initialSolution.iterateWithTermination(numIter) {
          current_solution: DataSet[PartialLassoSolution] => {
            val update_candidates = data map {
              new BestAtomFinder()
            } withBroadcastSet(current_solution, "solution")

            val update = update_candidates reduce {
              (left, right) => {
                left.max(right)
              }
            }

            val new_solution = current_solution map {
              new UpdateApproximation(beta, line_search)
            } withBroadcastSet(update, "update")

            // Check termination criterion
            val termination = new_solution filter { x =>
              x.dualityGap >= epsilon
            }

            (new_solution, termination)
          }
        }
      }
      case "GR" => {
        val matrices = data partitionCustom(new ColumnPartitioner, "idx") mapPartition {
          columns => {
            val mat = columns map {
              case ColumnVector(index, values) => MyMatrix(Array(values), Array(index))
            } reduce {
              (left, right) => MyMatrix(left.matrix ++ right.matrix, left.index ++ right.index)
            }
            Some(mat)
          }
        }
        initialSolution.iterateWithTermination(numIter) {
          current_solution: DataSet[PartialLassoSolution] => {
            val update_candidates = matrices map {
              new GroupBestAtomFinder()
            } withBroadcastSet(current_solution, "solution")

            val update = update_candidates reduce {
              (left, right) => {
                left.max(right)
              }
            }

            val new_solution = current_solution map {
              new UpdateApproximation(beta, line_search)
            } withBroadcastSet(update, "update")

            //TODO: Is duality_gap always a positive value ?
            val termination = new_solution filter { x =>
              x.dualityGap >= epsilon
            }

            (new_solution, termination)
          }
        }
      }
    }

    val out = iteration map {
      x => new LassoModel(x.sparseApproximation.idx, x.sparseApproximation.coef)
    }
    out
  }
}

// Case classes

// A partial solution of the Lasso problem.
@SerialVersionUID(123L)
case class PartialLassoSolution(
  residual: Array[Double],
  sparseApproximation: SparseApproximation,
  dualityGap: Double) extends Serializable

@SerialVersionUID(123L)
case class MyMatrix(matrix: Array[Array[Double]], index: Array[Int]) extends Serializable

@SerialVersionUID(123L)
case class ColumnVector(idx: Int, values: Array[Double]) extends Serializable

@SerialVersionUID(123L)
case class SparseApproximation(
  atoms: Array[Array[Double]],
  idx: Array[Int],
  coef: Array[Double]) extends Serializable {
  def compute(): DenseVector[Double] = {
    if (!isEmpty) {
      new DenseMatrix[Double](atoms(0).length, atoms.length, atoms.flatten) *
        new DenseVector[Double](coef)
    }
    else {
      DenseVector.zeros(0)
    }
  }

  def isEmpty: Boolean = {
    atoms.length == 0
  }

  def toSparseVector(length: Int): SparseVector[Double] = {
    val builder = new VectorBuilder[Double](length)
    val tuples = idx zip coef
    for (t <- tuples) {
      builder.add(t._1, t._2)
    }
    builder.toSparseVector
  }
}

@SerialVersionUID(123L)
object SparseApproximation extends Serializable {
  def initialApproximation: SparseApproximation = {
    new SparseApproximation(
      Array.empty[Array[Double]], Array.empty[Int], Array.empty[Double]
    )
  }
}

@SerialVersionUID(123L)
case class Update(atom: ColumnVector, value: Double) extends Serializable {
  def max(that: Update): Update = {
    if (abs(this.value) > abs(that.value)) this
    else that
  }
}

// Partitioners

class ColumnPartitioner extends Partitioner[Int] {
  override def partition(columnIndex: Int, numberOfPartitions: Int): Int = {
    columnIndex % numberOfPartitions
  }
}

// Rich map functions
@SerialVersionUID(123L)
class BestAtomFinder extends RichMapFunction[ColumnVector, Update] with Serializable {
  var residual: Array[Double] = null
  var size: Int = 0

  override def open(config: Configuration): Unit = {
    val sol = getRuntimeContext.getBroadcastVariable[PartialLassoSolution]("solution").get(0)
    residual = sol.residual
    size = residual.length
  }

  def map(in: ColumnVector): Update = {
    val grad = - blas.ddot(size, in.values, 1, residual, 1)
    Update(in, grad)
  }
}

class GroupBestAtomFinder() extends RichMapFunction[MyMatrix, Update] {
  var residual: DenseVector[Double] = null

  override def open(config: Configuration): Unit = {
    val sol = getRuntimeContext.getBroadcastVariable[PartialLassoSolution]("solution").get(0)
    residual = DenseVector(sol.residual)
  }

  def map(in: MyMatrix): Update = {
    val A = new DenseMatrix[Double](in.matrix(0).length, in.matrix.length, in.matrix.flatten)
    val grad = -A.t * residual
    val j = argmax(abs(grad))
    val index = in.index(j)
    Update(ColumnVector(index, in.matrix(j)), grad(j))
  }
}

class UpdateApproximation(beta: Double, line_search: Boolean = false)
  extends RichMapFunction[PartialLassoSolution, PartialLassoSolution] {
  var update: Update = null

  override def open(config: Configuration): Unit = {
    update = getRuntimeContext.getBroadcastVariable[Update]("update").get(0)
  }

  def map(tuple: PartialLassoSolution): PartialLassoSolution = {
    var new_residual: DenseVector[Double] = null
    var new_sol: SparseApproximation = null
    var approximation: DenseVector[Double] = null

    val PartialLassoSolution(residual, model, gap) = tuple
    val s_k: DenseVector[Double] = DenseVector[Double](update.atom.values) * (signum(-update
      .value) * beta)
    val A_temp = if (model.isEmpty) {
      s_k
    } else {
      approximation = model.compute()
      s_k - approximation
    }
    val duality_gap = A_temp.t * DenseVector(residual)

    // Compute step-size
    val gamma = if (line_search) {
      max(0.0, min(1.0, (duality_gap) / (A_temp.t * A_temp)))
    } else {
      val k = getIterationRuntimeContext.getSuperstepNumber - 1
      2.0 / (k + 2.0)
    }

    val v = gamma * beta * signum(-update.value)

    if (model.isEmpty) {
      new_residual = DenseVector(residual) - s_k * gamma
      new_sol = SparseApproximation(
        Array(update.atom.values),
        Array(update.atom.idx),
        Array(v)
      )
    }
    else {
      new_residual = DenseVector(residual) + gamma * (approximation - s_k)
      val i = model.idx.indexOf(update.atom.idx)
      val coef = DenseVector(model.coef) *= (1.0 - gamma)
      if (i == -1) {
        val new_idx = Array(update.atom.idx) ++ model.idx
        val new_coef = Array(v) ++ coef.toArray
        val new_atoms = Array(update.atom.values) ++ model.atoms
        new_sol = SparseApproximation(new_atoms, new_idx, new_coef)
      } else {
        coef(i) += v
        new_sol = SparseApproximation(model.atoms, model.idx, coef.toArray)
      }
    }
    println("Residual norm = " + norm(new_residual) + " Duality_gap = " + duality_gap)
    PartialLassoSolution(new_residual.toArray, new_sol, duality_gap)
  }
}
