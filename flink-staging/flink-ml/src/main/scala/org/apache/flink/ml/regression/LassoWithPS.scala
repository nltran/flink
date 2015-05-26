package org.apache.flink.ml.regression

import breeze.linalg._
import breeze.numerics._
import org.apache.flink.api.common.functions.{RichMapFunction, RichMapFunctionWithSSPServer}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ps.model.ParameterElement

/**
 * Created by Thomas Peel @ Eura Nova
 * on 20/05/15.
 */

class LassoWithPS(beta: Double,
                  numIter: Int,
                  normalize: Boolean = false,
                  line_search: Boolean = false,
                  epsilon: Double = 1e-3)
  extends Serializable {
  private var _coef: SparseVector[Double] = null

  def fit(data: DataSet[ColumnVector], target: DataSet[DenseVector[Double]]): LassoModel = {
    val Y = if (normalize) target map { x => breeze.linalg.normalize(x) } else target

    // Initialize parameter
    val initial = Y map { x => (x.copy, SparseParameterElement.empty) }

    val matrices = data partitionCustom(new ColumnPartitioner, "idx") mapPartition {
      columns => {
        val mat = columns map {
          case ColumnVector(index, values) => AtomSet(values.asDenseMatrix.t, Array(index))
        } reduce {
          (left, right) => AtomSet(DenseMatrix.horzcat(left.matrix, right.matrix), left.index ++ right.index)
        }
        Some(mat)
      }
    }

    val iteration = initial.iterateWithTermination(numIter) {
      residualApprox: DataSet[(DenseVector[Double], SparseParameterElement)] => {

        val residual = residualApprox map ( t => t._1)

        val residual_param_gap = matrices map {
          new UpdateParameter("alpha", beta, line_search)
        } withBroadcastSet(residual, "residual")

        val termination = residual_param_gap filter {
          tuple => abs(tuple._3) >= epsilon
        }

        val next = residual_param_gap map {
          tuple => (tuple._1, tuple._2)
        }

        (next, termination)
      }
    }

    val m = data.count.toInt

    this._coef = ((iteration collect) head)._2.getValue.toSparseVector(m)

    LassoModel(this._coef)
  }
}

// Case classes
case class LassoModel(coef: SparseVector[Double]) {
  override def toString(): String = {
    if (coef == null) "Empty model."
    else coef.toString()
  }
}

case class AtomSet(matrix: DenseMatrix[Double], index: Array[Int])

// Parameter elements

object SparseParameterElement {
  def empty(): SparseParameterElement = {
    new SparseParameterElement
  }
}

case class SparseParameterElement(clock: Int = 0,
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

class InitParameter(id: String) extends RichMapFunctionWithSSPServer[SparseParameterElement, SparseParameterElement] {
  def map(value: SparseParameterElement): SparseParameterElement = {
    println("##### Initializing parameter #####")
    update(id, value)
    value
  }
}

class GetParameter(id: String) extends RichMapFunctionWithSSPServer[DenseVector[Double], SparseParameterElement] {
  def map(in: DenseVector[Double]): SparseParameterElement = {
    println("Trying to get a parameter value from the parameter server.")
    val value = get(id).asInstanceOf[SparseParameterElement]
    println("The value is " + value)
    value
  }
}

class UpdateParameter(id: String, beta: Double, line_search: Boolean)
  extends RichMapFunctionWithSSPServer[AtomSet, (DenseVector[Double], SparseParameterElement, Double)] {
  var residual: DenseVector[Double] = null

  override def open(config: Configuration): Unit = {
    super.open(config)
    residual = getRuntimeContext.getBroadcastVariable[DenseVector[Double]]("residual").get(0).copy
  }

  def map(in: AtomSet): (DenseVector[Double], SparseParameterElement, Double) = {
    val iterationNumber = getIterationRuntimeContext.getSuperstepNumber
    val grad = -in.matrix.t * residual
    val j = argmax(abs(grad))
    val index = in.index(j)
    val atom = in.matrix(::, j).copy
    val gradJ = grad(j)


    var new_residual: DenseVector[Double] = null
    var new_sol: SparseApproximation = null

    var el: SparseParameterElement = get(id).asInstanceOf[SparseParameterElement]
    if (el == null) el = new SparseParameterElement
    val model = el.getValue

    val s_k: DenseVector[Double] = atom.copy * (signum(-gradJ) * beta)
    val A_temp = if (model.isEmpty()) s_k else s_k - model.atoms * model.coef
    val duality_gap = A_temp.t * residual

    // Compute step-size
    val gamma = if (line_search) {
      max(0.0, min(1.0, (duality_gap) / (A_temp.t * A_temp)))
    } else {
      val k = getIterationRuntimeContext.getSuperstepNumber - 1
      2.0 / (k + 2.0)
    }

    val v = gamma * beta * signum(-gradJ)

    if (model.isEmpty()) {
      new_residual = residual - s_k * gamma
      new_sol = SparseApproximation(atom.asDenseMatrix.t, Array(index), DenseVector[Double](v))
    }
    else {
      new_residual = residual + gamma * (model.atoms * model.coef - s_k)
      val idx = model.idx.indexOf(index)
      val coef: DenseVector[Double] = model.coef.copy * (1.0 - gamma)
      if (idx == -1) {
        val new_idx = (Array(index) ++ model.idx).clone()
        val new_coef = DenseVector[Double](Array(v) ++ coef.toArray).copy
        val new_atoms = DenseMatrix.horzcat(atom.copy.asDenseMatrix.t, model.atoms.copy)
        new_sol = SparseApproximation(new_atoms, new_idx, new_coef)
      } else {
        coef(idx) += v
        new_sol = SparseApproximation(model.atoms.copy, model.idx.clone(), coef)
      }
    }
    println("Residual norm = " + norm(new_residual) + " Duality_gap = " + duality_gap + "##### Actual clock : " + el.getClock)

    // Update parameter server
    val new_param = new SparseParameterElement(iterationNumber, new_sol)
    update(id, new_param)

    (new_residual, new_param, duality_gap)
  }
}
