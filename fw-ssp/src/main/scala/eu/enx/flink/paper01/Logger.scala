package eu.enx.flink.paper01

import java.nio.charset.StandardCharsets
import java.nio.file.{StandardOpenOption, Paths, Files}

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Created by Thomas Peel @ Eura Nova
 * on 30/06/15.
 */

class Logger(mapper: RichMapFunction, beta: Double) {
  /**
   * The path to the log file for each worker on HDFS looks like this:
   * /cluster_setting/beta_slack/sampleID/workerID.csv
   * TODO: getFilePath(workerID)
   * @return the path to the log file for this worker
   */

  var jobConf: Config = ConfigFactory.load("job.conf")

  def getLogFilePath: String = {
    val res = getLogFileDir + mapper.getRuntimeContext.getIndexOfThisSubtask + ".csv"
    res
  }

  def getLogFileDir: String = {
    val clusterSetting = jobConf.getInt("cluster.nodes")
    val rootdir = jobConf.getString("hdfs.result_rootdir")
    val slack = mapper.getRuntimeContext.getExecutionConfig.getSSPSlack

    val res = jobConf.getString("log.rootdir") + "/" + rootdir + "/" + clusterSetting + "/" +
      beta + "_" + slack + "_" + PaperJob.NOISE + "_" + PaperJob.SPARSITY + "/" + PaperJob
      .SAMPLE_ID +
      "/"
    res
  }

  /**
   * Writes the results to the disk
   */

  def writeToDisk(index: Int, residualNorm: Double, dualityGap: Double, t0: Long, t1: Long): Unit
  = {
    val dir: String = getLogFileDir
    val path: String = getLogFilePath
    val data: String = produceLogEntry(index, residualNorm, dualityGap, t1 - t0, t0)
    Files.createDirectories(Paths.get(dir))
    Files.write(Paths.get(path), (data + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption
      .APPEND, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
  }

  /**
   * Produces one line of log in the form (workerID, clock, atomID, worktime, residual)
   * @return a CSV String with the log entry
   */
  def produceLogEntry(atomIndex: Int, residual: Double, dualityGap: Double, elapsedTime: Long,
    startTime: Long): String = {
    val workerID = mapper.getRuntimeContext.getIndexOfThisSubtask
    val clock = mapper.getIterationRuntimeContext.getSuperstepNumber

    val res = List(workerID, clock, atomIndex, elapsedTime, residual, dualityGap, startTime).mkString(",")
    println("log entry: " + res)
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
}
