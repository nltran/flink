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

import breeze.linalg
import breeze.linalg._
import breeze.numerics._
import org.apache.flink.api.common.functions.RichMapFunctionWithSSPServer
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ps.model.ParameterElement

/**
 * Created by Thomas Peel @ Eura Nova
 * on 20/05/15.
 */

class LassoWithPS(
  beta: Double,
  numIter: Int,
  normalize: Boolean = false,
  line_search: Boolean = false,
  epsilon: Double = 1e-3)
  extends Serializable {
  private var _coef: SparseVector[Double] = null

  def fit(data: DataSet[ColumnVector], target: DataSet[Array[Double]]): LassoModel = {
    val Y = if (normalize) {
      target map { x => breeze.linalg.normalize(DenseVector(x)).toArray }
    } else {
      target
    }

    // Initialize parameter
    val initial = Y map { x => (x, SparseParameterElement.empty) }

    val matrices = data partitionCustom(new ColumnPartitioner, "idx") mapPartition {
      columns => {
        val mat = columns map {
          case ColumnVector(index, values) => AtomSet(Array(values), Array(index))
        } reduce {
          (
            left,
            right) => AtomSet(left.matrix ++ right.matrix, left.index ++ right
            .index)
        }
        Some(mat)
      }
    }

    val iteration = initial.iterateWithTermination(numIter) {
      residualApprox: DataSet[(Array[Double], SparseParameterElement)] => {

        val residual = residualApprox map (t => t._1)

        val residual_param_gap = matrices map {
          new UpdateParameter("alpha", beta, line_search)
        } withBroadcastSet(Y, "Y") withBroadcastSet(residual, "residual")

        // Seems that the duality gap in asynchronous setting is no longer a positive value at
        // each iteration.
        // Thus we take the absolute value of the dot product.
        // Is it correct ?
        val termination = residual_param_gap filter {
          tuple => abs(tuple._3) >= epsilon
        }

        val next = residual_param_gap map {
          tuple => (tuple._1, tuple._2)
        }

        (next, termination)
      }
    }

    val m = data.count().toInt

    this._coef = ((iteration collect) head)._2.getValue.toSparseVector(m)
    val s = ((iteration collect) head)._2.getValue

    LassoModel(s.idx, s.coef.toArray)
  }
}

// Case classes
case class LassoModel(index: Array[Int], data: Array[Double]) extends Serializable {
  override def toString: String = {
    if (index == null) "Empty model."
    else {
      assert(index.length == data.length)
      index.zip(data).mkString(" ")
    }
  }
}

case class AtomSet(matrix: Array[Array[Double]], index: Array[Int])

// Parameter elements

object SparseParameterElement {
  def empty: SparseParameterElement = {
    new SparseParameterElement
  }
}

case class SparseParameterElement(
  clock: Int = 0,
  value: SparseApproximation = SparseApproximation.initialApproximation)
  extends ParameterElement[SparseApproximation] {

  def getClock: Int = {
    clock
  }

  def getValue: SparseApproximation = {
    value
  }
}

// Rich map functions

class UpdateParameter(id: String, beta: Double, line_search: Boolean)
  extends RichMapFunctionWithSSPServer[AtomSet, (Array[Double], SparseParameterElement,
    Double)] {
  var Y: DenseVector[Double] = null

  override def open(config: Configuration): Unit = {
    super.open(config)
    Y = DenseVector(getRuntimeContext.getBroadcastVariable[Array[Double]]("Y").get(0))
  }

  def map(in: AtomSet): (Array[Double], SparseParameterElement, Double) = {
    val iterationNumber = getIterationRuntimeContext.getSuperstepNumber

    var new_residual: DenseVector[Double] = null
    var new_sol: SparseApproximation = null

    var el: SparseParameterElement = get(id).asInstanceOf[SparseParameterElement]
    if (el == null) el = new SparseParameterElement
    val model = el.getValue

    val approx = new DenseMatrix(model.atoms(0).length, model.atoms.length, model.atoms.flatten) *
      DenseVector(model.coef)

    val residual = if (model.isEmpty()) Y else Y - (approx)

    val A = new DenseMatrix[Double](in.matrix(0).length, in.matrix.length, in.matrix.flatten)
    val grad = -A.t * residual
    val j = argmax(abs(grad))
    val index = in.index(j)
    val atom = in.matrix(j)
    val gradJ = grad(j)

    val s_k: DenseVector[Double] = DenseVector(atom) * (signum(-gradJ) * beta)
    val A_temp = if (model.isEmpty()) s_k else s_k - approx
    // TODO: Check this !
    val duality_gap = A_temp.t * residual

    // Compute step-size
    val gamma = if (line_search) {
      max(0.0, min(1.0, duality_gap / (A_temp.t * A_temp)))
    } else {
      val k = getIterationRuntimeContext.getSuperstepNumber - 1
      2.0 / (k + 2.0)
    }

    val v = gamma * beta * signum(-gradJ)

    if (model.isEmpty()) {
      new_residual = residual - s_k * gamma
      new_sol = SparseApproximation(Array(atom), Array(index), Array(v))
    }
    else {
      new_residual = residual + gamma * (approx - s_k)
      val idx = model.idx.indexOf(index)
      val coef: DenseVector[Double] = DenseVector(model.coef) * (1.0 - gamma)
      if (idx == -1) {
        val new_idx = (Array(index) ++ model.idx).clone()
        val new_coef = Array(v) ++ coef.toArray
        val new_atoms = Array(atom) ++ model.atoms
        new_sol = SparseApproximation(new_atoms, new_idx, new_coef)
      } else {
        coef(idx) += v
        new_sol = SparseApproximation(model.atoms, model.idx, coef.toArray)
      }
    }
    println("Residual norm = " + norm(new_residual) + " Duality_gap = " + duality_gap + "##### " +
      "Actual clock : " + el.getClock)

    // Update parameter server
    val new_param = new SparseParameterElement(iterationNumber, new_sol)
    update(id, new_param)

    (new_residual.toArray, new_param, duality_gap)
  }
}
