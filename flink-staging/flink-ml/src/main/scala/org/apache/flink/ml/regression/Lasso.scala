package org.apache.flink.ml.regression

import breeze.linalg._
import breeze.numerics.{abs, signum}
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.flink.api.common.functions.{Partitioner, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * Created by Thomas Peel @ Eura Nova
 * on 18/05/15.
 */

class Lasso(beta: Double,
            numIter: Int,
            normalize: Boolean = false,
            line_search: Boolean = false,
            epsilon: Double = 1e-3,
            opt: String = "CD")
  extends Serializable {
  private var _coef: SparseVector[Double] = null
  private var _vec: ColumnVector = null

  def fit(data: DataSet[ColumnVector], target: DataSet[DenseVector[Double]]): Lasso = {
    val Y = if (normalize) target map { x => breeze.linalg.normalize(x) } else target

    val initialSolution = Y map { y => MyTuple(y, SparseApproximation.initialApproximation) }

    opt match {
      case "NA" => {

      }
      case "CD" => {
        val iteration = initialSolution.iterateWithTermination(numIter) {
          current_solution: DataSet[MyTuple] => {
            val update_candidates = data map {
              new BestAtomFinder()
            } withBroadcastSet(current_solution, "solution")

            val update = update_candidates reduce {
              (left, right) => {
                left min right
              }
            }

            val new_step = current_solution map {
              new UpdateApproximation(beta, line_search)
            } withBroadcastSet(update, "update")

            //TODO: Is duality_gap always a positive value ?
            val termination = new_step filter { tuple =>
              norm(tuple._2) >= epsilon
            }
            val new_solution = new_step map (tuple => tuple._1)

            (new_solution, termination)
          }
        }
        val m = data.count.toInt

        this._coef = (iteration first (1) map {
          approx => approx match {
            case MyTuple(residual, sol) => sol.toSparseVector(m)
          }
        } collect) head
      }
      case "GR" => {
        val matrices = data partitionCustom(new ColumnPartitioner, "idx") mapPartition {
          columns => {
            val mat = columns map {
              case ColumnVector(index, values) => MyMatrix(values.asDenseMatrix.t, Array(index))
            } reduce {
              (left, right) => MyMatrix(DenseMatrix.horzcat(left.matrix, right.matrix), left.index ++ right.index)
            }
            Some(mat)
          }
        }
        val iteration = initialSolution.iterateWithTermination(numIter) {
          current_solution: DataSet[MyTuple] => {
            val update_candidates = matrices map {
              new GroupBestAtomFinder()
            } withBroadcastSet(current_solution, "solution")

            val update = update_candidates reduce {
              (left, right) => {
                left min right
              }
            }

            val new_step = current_solution map {
              new UpdateApproximation(beta, line_search)
            } withBroadcastSet(update, "update")

            //TODO: Is duality_gap always a positive value ?
            val termination = new_step filter { tuple =>
              norm(tuple._2) >= epsilon
            }
            val new_solution = new_step map (tuple => tuple._1)

            (new_solution, termination)
          }
        }
        val m = data.count.toInt

        this._coef = (iteration first (1) map {
          approx => approx match {
            case MyTuple(residual, sol) => sol.toSparseVector(m)
          }
        } collect) head
      }
    }
    this
  }

  override def toString(): String = {
    if (this._coef == null) "Empty model."
    else this._coef.toString()
  }
}

// Case classes

case class MyTuple(residual: DenseVector[Double], sparseApproximation: SparseApproximation)

case class MyMatrix(matrix: DenseMatrix[Double], index: Array[Int])

case class VectorEntry(index: Int, value: Double)

case class MatrixEntry(row: Int, col: Int, value: Double)

case class TargetValue(value: Double)

case class ColumnVector(idx: Int, values: DenseVector[Double])

case class DataMatrix(m: DenseMatrix[Double])

case class IntermediateSolution(A: MyMatrix, approx: SparseApproximation)

case class SparseApproximation(atoms: DenseMatrix[Double], idx: Array[Int], coef: DenseVector[Double]) {
  def isEmpty(): Boolean = {
    atoms.cols == 0
  }

  def toSparseVector(length: Int): SparseVector[Double] = {
    val builder = new VectorBuilder[Double](length)
    val tuples = idx zip coef.toArray
    for (t <- tuples) {
      builder.add(t._1, t._2)
    }
    builder.toSparseVector
  }
}

object SparseApproximation {
  def initialApproximation: SparseApproximation = {
    new SparseApproximation(DenseMatrix.zeros[Double](0, 0), Array.empty[Int], DenseVector.zeros[Double](0))
  }
}

case class Update(atom: ColumnVector, value: Double) {
  def min(that: Update): Update = {
    if (this.value < that.value) this
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

class BestAtomFinder() extends RichMapFunction[ColumnVector, Update] {
  var residual: DenseVector[Double] = null

  override def open(config: Configuration): Unit = {
    val sol = getRuntimeContext.getBroadcastVariable[MyTuple]("solution").get(0)
    residual = sol.residual
  }

  def map(in: ColumnVector): Update = {
    val grad = -(in.values) dot residual
    Update(in, grad)
  }
}

class GroupBestAtomFinder() extends RichMapFunction[MyMatrix, Update] {
  var residual: DenseVector[Double] = null

  override def open(config: Configuration): Unit = {
    val sol = getRuntimeContext.getBroadcastVariable[MyTuple]("solution").get(0)
    residual = sol.residual
  }

  def map(in: MyMatrix): Update = {
    val grad = -in.matrix.t * residual
    val j = argmax(abs(grad))
    val index = in.index(j)
    Update(ColumnVector(index, in.matrix(::, j)), grad(j))
  }
}

class UpdateApproximation(beta: Double, line_search: Boolean = false)
  extends RichMapFunction[MyTuple, (MyTuple, Double)] {
  var update: Update = null

  override def open(config: Configuration): Unit = {
    update = getRuntimeContext.getBroadcastVariable[Update]("update").get(0)
  }

  def map(tuple: MyTuple): (MyTuple, Double) = {
    var new_residual: DenseVector[Double] = null
    var new_sol: SparseApproximation = null

    val MyTuple(residual, model) = tuple
    val s_k: DenseVector[Double] = update.atom.values * (signum(-update.value) * beta)
    val A_temp = if (model.isEmpty()) s_k else s_k - model.atoms * model.coef
    val duality_gap = A_temp.t * residual

    // Compute step-size
    val gamma = if (line_search) {
      max(0.0, min(1.0, (duality_gap) / (A_temp.t * A_temp)))
    } else {
      val k = getIterationRuntimeContext.getSuperstepNumber - 1
      2.0 / (k + 2.0)
    }

    val v = gamma * beta * signum(-update.value)

    if (model.isEmpty()) {
      new_residual = residual - s_k * gamma
      new_sol = SparseApproximation(update.atom.values.asDenseMatrix.t, Array(update.atom.idx), DenseVector[Double](v))
    }
    else {
      new_residual = residual + gamma * (model.atoms * model.coef - s_k)
      val i = model.idx.indexOf(update.atom.idx)
      val coef: DenseVector[Double] = model.coef * (1.0 - gamma)
      if (i == -1) {
        val new_idx = Array(update.atom.idx) ++ model.idx
        val new_coef = DenseVector[Double](Array(v) ++ coef.toArray)
        val new_atoms = DenseMatrix.horzcat(update.atom.values.asDenseMatrix.t, model.atoms)
        new_sol = SparseApproximation(new_atoms, new_idx, new_coef)
      } else {
        coef(i) += v
        new_sol = SparseApproximation(model.atoms, model.idx, coef)
      }
    }
    println("Residual norm = " + norm(new_residual) + " Duality_gap = " + duality_gap)
    (MyTuple(new_residual, new_sol), duality_gap)
  }
}