package eu.enx.flink.paper01

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import breeze.linalg._
import breeze.numerics._
import com.github.fommil.netlib.BLAS.{ getInstance => blas }
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.RichMapFunctionWithSSPServer
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Created by Thomas Peel @ Eura Nova
 * on 30/06/15.
 */

class LassoWithPS(
  beta: Double,
  numIter: Int,
  normalize: Boolean = false,
  line_search: Boolean = false,
  epsilon: Double = 1e-3,
  opt: String = "CD")
  extends Serializable {

  def fit(
    data: DataSet[ColumnVector],
    target: DataSet[Array[Double]],
    log: Boolean = false,
    slack: Int): DataSet[LassoModel] = {
    val Y = if (normalize) {
      target map { x => breeze.linalg.normalize(DenseVector(x)).toArray }
    } else {
      target
    }

    // Initialize parameter
    val initial = Y map { x => (x, SparseParameterElement.empty) }
    //    val plainIterationMode = if (ConfigFactory.load("job.conf").getString("iteration")
    // .equals("plain")) true else false
    //    val plainIterationMode = if (ConfigFactory.load("job.conf").getInt("slack").equals
    // ("plain")) true else false

    val iteration = opt match {
      // Coordinate-wise
      case "CD" => {
        val splittedData = data.partitionCustom(new ColumnPartitioner, "idx")
          .mapPartition(colums => Some(colums.toArray))

        if (slack == 0) {

          initial.iterateWithTermination(numIter) {
            residualApprox: DataSet[(Array[Double], SparseParameterElement)] => {

              val residual = residualApprox map (t => t._1)

              val residual_param_gap = splittedData.map {
                new UpdateParameterCD("alpha", beta, line_search, epsilon, numIter, log)
              }.withBroadcastSet(Y, "Y").withBroadcastSet(residual, "residual")

              // Seems that the duality gap in asynchronous setting is no longer a positive value at
              // each iteration.
              // Thus we take the absolute value of the dot product.
              // Is it correct ?
              val termination = residual_param_gap filter {
                tuple => tuple._3 >= epsilon
              }

              val next = residual_param_gap map {
                tuple => (tuple._1, tuple._2)
              }

              (next, termination)
            }
          }
        }
        else {
          initial.iterateWithSSPWithTermination(numIter) {
            residualApprox: DataSet[(Array[Double], SparseParameterElement)] => {

              val residual = residualApprox map (t => t._1)

              val residual_param_gap = splittedData.map {
                new UpdateParameterCD("alpha", beta, line_search, epsilon, numIter, log)
              }.withBroadcastSet(Y, "Y").withBroadcastSet(residual, "residual")

              // Seems that the duality gap in asynchronous setting is no longer a positive value at
              // each iteration.
              // Thus we take the absolute value of the dot product.
              // Is it correct ?
              val termination = residual_param_gap filter {
                tuple => tuple._3 >= epsilon
              }

              val next = residual_param_gap map {
                tuple => (tuple._1, tuple._2)
              }

              (next, termination)
            }
          }
        }
      }
      // Block coordinate-wise
      case "GR" => {
        val matrices = data partitionCustom(new ColumnPartitioner, "idx") mapPartition {
          columns => {
            val mat = columns map {
              case ColumnVector(index, values) => AtomSet(Array(values), Array(index))
            } reduce {
              (
                left,
                right) => AtomSet(left.matrix ++ right.matrix, left.index ++ right.index)
            }
            Some(mat)
          }
        }

        if (slack == 0) {
          initial.iterateWithTermination(numIter) {
            residualApprox: DataSet[(Array[Double], SparseParameterElement)] => {

              val residual = residualApprox map (t => t._1)

              val residual_param_gap = matrices map {
                new UpdateParameter("alpha", beta, line_search, epsilon, numIter, log)
              } withBroadcastSet(Y, "Y") withBroadcastSet(residual, "residual")

              // Seems that the duality gap in asynchronous setting is no longer a positive value at
              // each iteration.
              // Thus we take the absolute value of the dot product.
              // Is it correct ?
              val termination = residual_param_gap filter {
                tuple => tuple._3 >= epsilon
              }

              val next = residual_param_gap map {
                tuple => (tuple._1, tuple._2)
              }

              (next, termination)
            }
          }
        }

        else {
          initial.iterateWithSSPWithTermination(numIter) {
            residualApprox: DataSet[(Array[Double], SparseParameterElement)] => {

              val residual = residualApprox map (t => t._1)

              val residual_param_gap = matrices map {
                new UpdateParameter("alpha", beta, line_search, epsilon, numIter, log)
              } withBroadcastSet(Y, "Y") withBroadcastSet(residual, "residual")

              // Seems that the duality gap in asynchronous setting is no longer a positive value at
              // each iteration.
              // Thus we take the absolute value of the dot product.
              // Is it correct ?
              val termination = residual_param_gap filter {
                tuple => tuple._3 >= epsilon
              }

              val next = residual_param_gap map {
                tuple => (tuple._1, tuple._2)
              }

              (next, termination)
            }
          }
        }
      }
    }

    val out = iteration map {
      x => new LassoModel(x._2.getValue.idx, x._2.getValue.coef)
    }
    out
  }
}

@SerialVersionUID(123L)
class UpdateParameter(
  id: String,
  beta: Double,
  line_search: Boolean,
  epsilon: Double,
  maxIter: Int,
  log: Boolean,
  mode: String = "CD")
  extends RichMapFunctionWithSSPServer[AtomSet, (Array[Double], SparseParameterElement,
    Double)] with Serializable {
  var Y: DenseVector[Double] = null
  var logger: Logger = null

  override def open(config: Configuration): Unit = {
    super.open(config)
    Y = DenseVector(getRuntimeContext.getBroadcastVariable[Array[Double]]("Y").get(0))
    if (log) {
      logger = new Logger(this, beta)
    }
    println("Slack is: " + getRuntimeContext.getExecutionConfig.getSSPSlack)
  }

  def map(in: AtomSet): (Array[Double], SparseParameterElement, Double) = {
    val t0 = System.nanoTime

    val iterationNumber = getIterationRuntimeContext.getSuperstepNumber

    var new_residual: DenseVector[Double] = null
    var new_sol: SparseApproximation = null

    var el: SparseParameterElement = get(id).asInstanceOf[SparseParameterElement]
    if (el == null) el = new SparseParameterElement
    val model = el.getValue

    val approx = model.compute()

    val residual = if (model.isEmpty) Y else Y - approx

    val A = new DenseMatrix[Double](in.matrix(0).length, in.matrix.length, in.matrix.flatten)
    val grad = -A.t * residual
    val j = argmax(abs(grad))
    val index = in.index(j)
    val atom = in.matrix(j)
    val gradJ = grad(j)

    val s_k: DenseVector[Double] = DenseVector(atom) * (signum(-gradJ) * beta)
    val A_temp = if (model.isEmpty) s_k else s_k - approx
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

    if (model.isEmpty) {
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

    val residualNorm = norm(new_residual)

    println("Worker = " + getRuntimeContext.getIndexOfThisSubtask + " Residual norm = " +
      residualNorm + " Duality_gap = "
      + duality_gap +
      "##### " +
      "Actual clock : " + el.getClock)

    // Update parameter server
    val new_param = new SparseParameterElement(iterationNumber, new_sol)
    update(id, new_param)

    val t1 = System.nanoTime

    // Logs
    if (log) {
      logger.writeToDisk(index, residualNorm, duality_gap, t0, t1)
    }
    (new_residual.toArray, new_param, duality_gap)
  }

  override def close() = {
    super.close()
  }
}

@SerialVersionUID(123L)
class UpdateParameterCD(
  id: String,
  beta: Double,
  line_search: Boolean,
  epsilon: Double,
  maxIter: Int,
  log: Boolean)
  extends RichMapFunctionWithSSPServer[Array[ColumnVector], (Array[Double], SparseParameterElement,
    Double)] with Serializable {

  var Y: DenseVector[Double] = null
  var logger: Logger = null
  var size: Int = 0

  override def open(config: Configuration): Unit = {
    super.open(config)
    Y = DenseVector(getRuntimeContext.getBroadcastVariable[Array[Double]]("Y").get(0))
    if (log) {
      logger = new Logger(this, beta)
    }
    size = Y.length
    println("Slack is: " + getRuntimeContext.getExecutionConfig.getSSPSlack)
  }

  def map(in: Array[ColumnVector]): (Array[Double], SparseParameterElement, Double) = {
    val t0 = System.nanoTime

    val iterationNumber = getIterationRuntimeContext.getSuperstepNumber

    var new_residual: DenseVector[Double] = null
    var new_sol: SparseApproximation = null

    var el: SparseParameterElement = get(id).asInstanceOf[SparseParameterElement]
    if (el == null) el = new SparseParameterElement
    val model = el.getValue

    val approx = model.compute()

    val residual = if (model.isEmpty) Y else Y - approx

    // Select the best atom
    val (atom, index, grad) = in.map {
      x => (x.values, x.idx, -blas.ddot(size, x.values, 1, residual.toArray, 1))
    }.reduce {
      (left, right) => if (abs(left._3) > abs(right._3)) left else right
    }

    // Update using the selected atom
    val s_k: DenseVector[Double] = DenseVector(atom) * (signum(-grad) * beta)
    val A_temp = if (model.isEmpty) s_k else s_k - approx
    // TODO: Check this !
    val duality_gap = A_temp.t * residual

    // Compute step-size
    val gamma = if (line_search) {
      max(0.0, min(1.0, duality_gap / (A_temp.t * A_temp)))
    } else {
      val k = getIterationRuntimeContext.getSuperstepNumber - 1
      2.0 / (k + 2.0)
    }

    val v = gamma * beta * signum(-grad)

    if (model.isEmpty) {
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
    val residualNorm = norm(new_residual)

    println("Worker = " + getRuntimeContext.getIndexOfThisSubtask + " Residual norm = " +
      residualNorm + " Duality_gap = " + duality_gap + "##### " +
      "Actual clock : " + el.getClock)

    // Update parameter server
    val new_param = new SparseParameterElement(iterationNumber, new_sol)
    update(id, new_param)

    val t1 = System.nanoTime

    // Logs
    if (log) {
      logger.writeToDisk(index, residualNorm, duality_gap, t0, t1)
    }
    (new_residual.toArray, new_param, duality_gap)
  }

  override def close() = {
    super.close()
  }
}
