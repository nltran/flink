package eu.enx.flink.paper01

/**
 * Created by Thomas Peel @ Eura Nova
 * on 20/05/15.
 */

import com.typesafe.config.{ConfigFactory}

object PaperJob {
  val jobConf = ConfigFactory.load("job.conf")
  val NOISE = jobConf.getString("noise")
  val SPARSITY = jobConf.getString("sparsity")
  val SAMPLE_ID = jobConf.getString("sampleID")
}
