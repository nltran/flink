package org.apache.flink.ml.regression

import breeze.linalg._
import breeze.numerics._
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.flink.api.common.functions.{RichMapFunction, RichMapFunctionWithSSPServer}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ps.model.ParameterElement
import org.apache.hadoop.fs.{FileSystem, Path}

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
    val Y = if (normalize) target map { x => breeze.linalg.normalize(x.copy) } else target

    // Initialize parameter
    val initial = Y map { x => (x, SparseParameterElement.empty) }

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
          new UpdateParameter("alpha", beta, line_search, epsilon, numIter)
        } withBroadcastSet(Y, "Y") withBroadcastSet(residual, "residual")

        // Seems that the duality gap in asynchronous setting is no longer a positive value at each iteration.
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

    LassoModel(this._coef)
  }
}

// Case classes
case class LassoModel(coef: SparseVector[Double]) {
  override def toString: String = {
    if (coef == null) "Empty model."
    else coef.toString()
  }
}

case class AtomSet(matrix: DenseMatrix[Double], index: Array[Int])

// Parameter elements

object SparseParameterElement {
  def empty: SparseParameterElement = {
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

class UpdateParameter(id: String, beta: Double, line_search: Boolean, epsilon:Double, maxIter:Int)
  extends RichMapFunctionWithSSPServer[AtomSet, (DenseVector[Double], SparseParameterElement, Double)] {
  var Y: DenseVector[Double] = null
  var jobConf:Config = null;
  var logBuf:scala.collection.mutable.ListBuffer[String] = null

  override def open(config: Configuration): Unit = {
    super.open(config)
    Y = getRuntimeContext.getBroadcastVariable[DenseVector[Double]]("Y").get(0).copy
    jobConf = ConfigFactory.load("job.conf")
    if(logBuf == null) {
      logBuf = scala.collection.mutable.ListBuffer.empty[String]
    }
  }

  /**
   * The path to the log file for each worker on HDFS looks like this:
   * /cluster_setting/beta_slack/sampleID/workerID.csv
   * TODO: getFilePath(workerID)
   * @return the path to the log file for this worker
   */
  def getLogFilePath:String = {
    val clusterSetting = jobConf.getInt("cluster.nodes")
    val rootdir = jobConf.getString("hdfs.result_rootdir")
    val slack = getRuntimeContext.getExecutionConfig.getSSPSlack
    val workerID = getRuntimeContext.getIndexOfThisSubtask
    val sampleID = 0

    val res = "/" + rootdir +  "/" + clusterSetting + "/" + beta+"_" + slack+"/"+ sampleID+ "/" + workerID + ".csv"
    res
  }

  def write(uri: String, filePath: String, data: List[String]): Unit = {
    def values = for(i <- data) yield i

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val path = new Path(filePath)
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)

    if(fs.exists(path)) {
      fs.delete(path, false)
    }

    val os = fs.create(path)
    data.foreach(a => os.write(a.getBytes()))

    fs.close()
  }

  /**
   * Produces one line of log in the form (workerID, clock, atomID, worktime, residual)
   * @return a CSV String with the log entry
   */
  def produceLogEntry(atomIndex:Int, dualityGap:Double, time:Long):String = {
    val workerID = getRuntimeContext.getIndexOfThisSubtask
    val clock = getIterationRuntimeContext.getSuperstepNumber

    val res = workerID + ","+clock+","+atomIndex+"," + time + "," +dualityGap
    println("log entry: " + res)
    res
  }

  /**
   * Given the current iteration and residual, returns true if the algorithm has converged
   * @return true if the algorithm has converged
   */
  def isConverged(maxIterations: Int, duality_gap:Double, epsilon:Double): Boolean = {
    val converged = if (getIterationRuntimeContext.getSuperstepNumber == maxIterations || duality_gap <= epsilon) true else false
      converged

  }

  def map(in: AtomSet): (DenseVector[Double], SparseParameterElement, Double) = {
    val t0 = System.nanoTime

    val tt = getLogFilePath
    val iterationNumber = getIterationRuntimeContext.getSuperstepNumber

    var new_residual: DenseVector[Double] = null
    var new_sol: SparseApproximation = null

    var el: SparseParameterElement = get(id).asInstanceOf[SparseParameterElement]
    if (el == null) el = new SparseParameterElement
    val model = el.getValue

    val residual = if (model.isEmpty()) Y else Y - (model.atoms * model.coef)

    val grad = -in.matrix.t * residual
    val j = argmax(abs(grad))
    val index = in.index(j)
    val atom = in.matrix(::, j).copy
    val gradJ = grad(j)

    val s_k: DenseVector[Double] = atom.copy * (signum(-gradJ) * beta)
    val A_temp = if (model.isEmpty()) s_k else s_k - model.atoms * model.coef
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

    val t1 = System.nanoTime

    logBuf += produceLogEntry(index,norm(new_residual), t1-t0)

    if (isConverged(maxIter, duality_gap, epsilon)) {
      println("writing to hdfs")
      write(jobConf.getString("hdfs.uri"), getLogFilePath, logBuf.toList)
    }
    (new_residual, new_param, duality_gap)
  }

  override def close() = {
    super.close();
  }


}
