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
import breeze.numerics._
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.functions.RichMapFunctionWithSSPServer
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ps.model.ParameterElement
import org.apache.hadoop.fs.{FileSystem, Path}
import java.nio.file.{StandardOpenOption, OpenOption, Paths, Files}
import java.nio.charset.StandardCharsets
import java.io.File

/**
 * Created by Thomas Peel @ Eura Nova
 * on 20/05/15.
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
    log: Boolean = false): DataSet[LassoModel] = {
    val Y = if (normalize) {
      target map { x => breeze.linalg.normalize(DenseVector(x)).toArray }
    } else {
      target
    }

    // Initialize parameter
    val initial = Y map { x => (x, SparseParameterElement.empty) }
    val plainIterationMode = if (ConfigFactory.load("job.conf").getString("iteration").equals("plain")) true else false

    val iteration = opt match {
      // Coordinate-wise
      case "CD" => {
        val splittedData = data.partitionCustom(new ColumnPartitioner, "idx")
          .mapPartition(colums => Some(colums.toArray))

        if (plainIterationMode){

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
                tuple => abs(tuple._3) >= epsilon
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
                tuple => abs(tuple._3) >= epsilon
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

        if (plainIterationMode) {
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
                tuple => abs(tuple._3) >= epsilon
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
                tuple => abs(tuple._3) >= epsilon
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

// Case classes
case class LassoModel(var index: Array[Int], var data: Array[Double]) extends Serializable {
  override def toString: String = {
    if (index == null) "Empty model."
    else {
      assert(index.length == data.length)
      index.zip(data).map {
        case (a: Int, b: Double) => a + "," + b
      }.mkString("\n")
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
  var jobConf: Config = null
  var logBuf: scala.collection.mutable.ListBuffer[String] = null

  override def open(config: Configuration): Unit = {
    super.open(config)
    Y = DenseVector(getRuntimeContext.getBroadcastVariable[Array[Double]]("Y").get(0))
    if (log) {
      jobConf = ConfigFactory.load("job.conf")
      if (logBuf == null) logBuf = scala.collection.mutable.ListBuffer.empty[String]
    }
    println("Slack is: "+ getRuntimeContext.getExecutionConfig.getSSPSlack)
  }

  def map(in: AtomSet): (Array[Double], SparseParameterElement, Double) = {
    val t0 = System.nanoTime

    if (log) {
      val tt = getLogFilePath
    }

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

    println("Worker = "+ getRuntimeContext.getIndexOfThisSubtask+ " Residual norm = " +
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
      //logBuf += produceLogEntry(index, norm(new_residual), t1 - t0)

      //if (isConverged(maxIter, duality_gap, epsilon)) {
        //println("writing to: "+ getLogFilePath)
//        write(jobConf.getString("hdfs.uri"), getLogFilePath, logBuf.toList)
    writeToDisk(getLogFileDir, getLogFilePath,
      produceLogEntry(index, residualNorm, duality_gap, t1 - t0))
      //}
    }
    (new_residual.toArray, new_param, duality_gap)
  }

  /**
   * The path to the log file for each worker on HDFS looks like this:
   * /cluster_setting/beta_slack/sampleID/workerID.csv
   * TODO: getFilePath(workerID)
   * @return the path to the log file for this worker
   */
  def getLogFilePath: String = {
    val clusterSetting = jobConf.getInt("cluster.nodes")
    val rootdir = jobConf.getString("hdfs.result_rootdir")
    val slack = getRuntimeContext.getExecutionConfig.getSSPSlack
    val workerID = getRuntimeContext.getIndexOfThisSubtask
    val sampleID = 0

    val res = jobConf.getString("log.rootdir") + "/" + rootdir + "/" + clusterSetting + "/" + beta + "_" + slack + "/" + sampleID +
      "/" + workerID + ".csv"
    res
  }

  def getLogFileDir: String = {
    val clusterSetting = jobConf.getInt("cluster.nodes")
    val rootdir = jobConf.getString("hdfs.result_rootdir")
    val slack = getRuntimeContext.getExecutionConfig.getSSPSlack
    val workerID = getRuntimeContext.getIndexOfThisSubtask
    val sampleID = 0

    val res = jobConf.getString("log.rootdir") +  "/" + rootdir + "/" + clusterSetting + "/" + beta + "_" + slack + "/" + sampleID
    res
  }

  def write(uri: String, filePath: String, data: List[String]): Unit = {
    def values = for (i <- data) yield i

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val path = new Path(filePath)
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)

    if (fs.exists(path)) {
      fs.delete(path, false)
    }

    val os = fs.create(path)
    data.foreach(a => os.write((a + "\n").getBytes()))

    fs.close()
  }

  /**
   * Writes the results to the disk
    * @param path
   * @param data
   */

  def writeToDisk(dir: String, path:String, data:String): Unit = {
    //val file = new File(path)
    //file.getParentFile.mkdirs()
    //file.createNewFile()
    Files.createDirectories(Paths.get(dir))
    Files.write(Paths.get(path),(data+"\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption
      .APPEND, StandardOpenOption.CREATE, StandardOpenOption.WRITE )
  }

  /**
   * Produces one line of log in the form (workerID, clock, atomID, worktime, residual)
   * @return a CSV String with the log entry
   */
  def produceLogEntry(atomIndex: Int, residual: Double, dualityGap: Double, time: Long): String = {
    val workerID = getRuntimeContext.getIndexOfThisSubtask
    val clock = getIterationRuntimeContext.getSuperstepNumber

    val res = List(workerID, clock, atomIndex, time, residual, dualityGap).mkString(",")
    println("log entry: " + res)
    res
  }

  /**
   * Given the current iteration and residual, returns true if the algorithm has converged
   * @return true if the algorithm has converged
   */
  def isConverged(maxIterations: Int, duality_gap: Double, epsilon: Double): Boolean = {
    val converged = if (getIterationRuntimeContext.getSuperstepNumber == maxIterations ||
      duality_gap <= epsilon) {
      true
    } else {
      false
    }
    converged

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
  var jobConf: Config = null
  var logBuf: scala.collection.mutable.ListBuffer[String] = null
  var size: Int = 0

  override def open(config: Configuration): Unit = {
    super.open(config)
    Y = DenseVector(getRuntimeContext.getBroadcastVariable[Array[Double]]("Y").get(0))
    size = Y.length
    if (log) {
      jobConf = ConfigFactory.load("job.conf")
      if (logBuf == null) logBuf = scala.collection.mutable.ListBuffer.empty[String]
    }
    println("Slack is: "+ getRuntimeContext.getExecutionConfig.getSSPSlack)
  }

  def map(in: Array[ColumnVector]): (Array[Double], SparseParameterElement, Double) = {
    val t0 = System.nanoTime

    if (log) {
      val tt = getLogFilePath
    }

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
    println("Worker = "+ getRuntimeContext.getIndexOfThisSubtask+" Residual norm = " + norm
      (new_residual) + " Duality_gap = " + duality_gap + "##### " +
      "Actual clock : " + el.getClock)

    // Update parameter server
    val new_param = new SparseParameterElement(iterationNumber, new_sol)
    update(id, new_param)

    val t1 = System.nanoTime

    if (log) {
      logBuf += produceLogEntry(index, norm(new_residual), duality_gap, t1 - t0)

      if (isConverged(maxIter, duality_gap, epsilon)) {
        println("writing to: " + getLogFilePath)
//        write(jobConf.getString("hdfs.uri"), getLogFilePath, logBuf.toList)
        writeToDisk(getLogFileDir, getLogFilePath, logBuf.toList)
      }

    }
    (new_residual.toArray, new_param, duality_gap)
  }

  /**
   * The path to the log file for each worker on HDFS looks like this:
   * /cluster_setting/beta_slack/sampleID/workerID.csv
   * TODO: getFilePath(workerID)
   * @return the path to the log file for this worker
   */
  def getLogFilePath: String = {
    val clusterSetting = jobConf.getInt("cluster.nodes")
    val rootdir = jobConf.getString("hdfs.result_rootdir")
    val slack = getRuntimeContext.getExecutionConfig.getSSPSlack
    val workerID = getRuntimeContext.getIndexOfThisSubtask
    val sampleID = 0

    val res = jobConf.getString("log.rootdir") + "/" + rootdir + "/" + clusterSetting + "/" + beta + "_" + slack + "/" + sampleID +
      "/" + workerID + ".csv"
    res
  }

  def getLogFileDir: String = {
    val clusterSetting = jobConf.getInt("cluster.nodes")
    val rootdir = jobConf.getString("hdfs.result_rootdir")
    val slack = getRuntimeContext.getExecutionConfig.getSSPSlack
    val workerID = getRuntimeContext.getIndexOfThisSubtask
    val sampleID = 0

    val res = jobConf.getString("log.rootdir") +  "/" + rootdir + "/" + clusterSetting + "/" + beta + "_" + slack + "/" + sampleID
    res
  }

  def write(uri: String, filePath: String, data: List[String]): Unit = {
    def values = for (i <- data) yield i

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val path = new Path(filePath)
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)

    if (fs.exists(path)) {
      fs.delete(path, false)
    }

    val os = fs.create(path)
    data.foreach(a => os.write((a + "\n").getBytes()))

    fs.close()
  }

  /**
   * Produces one line of log in the form (workerID, clock, atomID, worktime, residual)
   * @return a CSV String with the log entry
   */
  def produceLogEntry(atomIndex: Int, residual: Double, dualityGap: Double, time: Long): String = {
    val workerID = getRuntimeContext.getIndexOfThisSubtask
    val clock = getIterationRuntimeContext.getSuperstepNumber

    val res = List(workerID, clock, atomIndex, time, residual, dualityGap).mkString(",")
    println("log entry: " + res)
    res
  }

  /**
   * Given the current iteration and residual, returns true if the algorithm has converged
   * @return true if the algorithm has converged
   */
  def isConverged(maxIterations: Int, duality_gap: Double, epsilon: Double): Boolean = {
    val converged = if (getIterationRuntimeContext.getSuperstepNumber == maxIterations ||
      duality_gap <= epsilon) {
      true
    } else {
      false
    }
    converged

  }

  /**
   * Writes the results to the disk
   * @param path
   * @param data
   */

  def writeToDisk(dir: String, path:String, data:List[String]): Unit = {
    val file = new File(path)
    file.getParentFile.mkdirs()
    file.createNewFile()
    Files.createDirectories(Paths.get(dir))
    data.foreach( a => Files.write(Paths.get(path),(a+"\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND, StandardOpenOption.CREATE, StandardOpenOption.WRITE ))
  }

  override def close() = {
    super.close()
  }
}
