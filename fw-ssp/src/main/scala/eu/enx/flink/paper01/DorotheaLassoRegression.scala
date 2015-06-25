package eu.enx.flink.paper01

import breeze.linalg.{DenseVector, VectorBuilder, normalize}
import breeze.stats.distributions.Gaussian
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE
import org.apache.flink.ml.regression.{Lasso, ColumnVector, LassoWithPS}

/**
 * Created by Thomas Peel @ Eura Nova
 * on 12/06/15.
 */
object DorotheaLassoRegression {
  def main(args: Array[String]): Unit = {
    val EPSILON = 1e-3
    val PARALLELISM = ConfigFactory.load("job.conf").getInt("cluster.nodes")
    val NUMITER = 100
    val NORMALIZE = true
    val LINESEARCH = true
    val OPT = "CD"

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(PARALLELISM)
    env.setSSPSlack(ConfigFactory.load("job.conf").getInt("slack"))

    val beta = ConfigFactory.load("job.conf").getDouble("beta")

    val fw = new Lasso(
      beta = beta,
      numIter = NUMITER,
      normalize = NORMALIZE,
      line_search = LINESEARCH,
      epsilon = EPSILON,
      opt = OPT)

    val y = env.readTextFile(
//      "hdfs://10.0.3.109/user/paper01/data/dorothea/dorothea_train_sub.labels"
      "hdfs://10.0.3.109/user/paper01/data/dorothea/test_target.csv"
    ).setParallelism(1).map(x => x.toDouble)

    val Y = y.reduceGroup(iterator => iterator.toArray)

    val dimension = y.count.toInt

    val csv = env.readCsvFile[(Int, String)](
//      "hdfs://10.0.3.109/user/paper01/data/dorothea/dorothea_train_sub.csv",
      "hdfs://10.0.3.109/user/paper01/data/dorothea/test_data.csv",
      ignoreFirstLine = true)

    val bagOfEntry = csv.flatMap {
      tuple => {
        val id = tuple._1
        val values = tuple._2.split(" ").map(x => x.toInt)
        List.fill(values.length)(id).zip(values)
      }
    }.groupBy(1)

    val cols = bagOfEntry.reduceGroup(
      iterator => {
        val s = iterator.toSeq
        val id = s(0)._2
        val indices = s map { case (a, b) => a }
        val v = new VectorBuilder[Double](dimension)
        for (c <- indices) {
          v.add(c, 1)
        }
        ColumnVector(id, v.toDenseVector.toArray)
      }
    )

    val model = fw.fit(cols, Y, log = true ).first(1)

    model.writeAsText(
      "hdfs://10.0.3.109/user/paper01/data/dorothea/result01.txt",
      OVERWRITE
    )

    env.execute()
  }

  def columnGenerator(dim: Int, num: Int, dis: String): Stream[ColumnVector] = {
    var gen: Stream[ColumnVector] = null
    dis match {
      case "uniform" =>
        gen = Stream.range(0, num).map {
          i => ColumnVector(i, normalize(DenseVector.rand[Double](dim)).toArray)
        }
      case "gaussian" =>
        gen = Stream.range(0, num).map {
          i => {
            val g = Gaussian(i % dim, 1)
            val h = DenseVector((0 until dim).map(i => g.probability(i, i + 1)).toArray)
            ColumnVector(i, normalize(h).toArray)
          }
        }
    }
    gen
  }

  def signalGenerator(
    colVec: DataSet[ColumnVector],
    noise: Double,
    coeff: DataSet[SparseEntry]): DataSet[Array[Double]] = {
    val joinedDataSet = colVec.joinWithTiny(coeff).where("idx").equalTo("index")
    val ret = joinedDataSet.map {
      new ElementWiseMul
    }.reduce {
      (left, right) => (DenseVector(left) + DenseVector(right)).toArray
    }
    ret.map {
      x => normalize(DenseVector(x) + DenseVector.rand[Double](x.length) *= noise).toArray
    }
  }

  def sparseEntryGenerator(indices: Array[Int], coeff: Array[Double]): Array[SparseEntry] = {
    indices.zip(coeff).map(x => SparseEntry(x._1, x._2))
  }

  case class SparseEntry(index: Int, value: Double)

  case class SparseBinaryData(id: Int, values: Array[Int]) {
    override def toString: String = {
      val l = values.length
      val s = if (l <= 10) {
        "SparseBinaryVector(" + id + ", [" + values.mkString(" ") + "])"
      } else {
        "SparseBinaryVector(" + id + ", [" + values.slice(0, 3).mkString(" ") + " ... " +
          values.slice(l - 3, l).mkString(" ") + "])"
      }
      s
    }
  }

  class ElementWiseMul extends RichMapFunction[(ColumnVector, SparseEntry), Array[Double]] {
    override def map(value: (ColumnVector, SparseEntry)): Array[Double] = {
      (DenseVector(value._1.values) *= value._2.value).toArray
    }
  }

}
