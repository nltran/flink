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

package eu.enx.flink.paper01

import breeze.linalg._
import breeze.numerics._
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.ml.regression.{LassoWithPS, ColumnVector, Lasso}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration


/**
 * This example implements a basic Lasso Regression with a distributed Franck-Wolfe optimization method.
 *
 * Created by Thomas Peel @ Eura Nova
 * on 18/05/15.
 */

object LassoRegression {
  def main(args: Array[String]) {
    val EPSILON = 1e-4
    val PARALLELISM = 2
    val NUMITER = 1
    val NORMALIZE = false
    val LINESEARCH = true
    val NOISY = false
    val OPT = "GR"

    val dimension = 64
    //    val size = dimension
    val size = 64

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(PARALLELISM)

    //    val X: DenseMatrix[Double] = DenseMatrix.eye[Double](dimension)
    val X: DenseMatrix[Double] = DenseMatrix.rand[Double](dimension, size)
    val alpha = SparseVector.zeros[Double](size)
    alpha.update(1, 0.45)
    alpha.update(2, 0.18)
    alpha.update(3, 0.98)
    alpha.update(4, 0.67)

    val beta = 1.0 //sum(abs(alpha))
//
//    val noise = if (NOISY) DenseVector.rand[Double](dimension) * 0.001 else DenseVector.zeros[Double](dimension)
//
//    val y = X * alpha + noise
//
//    val Y = env.fromElements(y)
//
//    val columns = env.fromCollection({
//      0 until size
//    } map { i => ColumnVector(i, X(::, i).copy)})

    val fw = new LassoWithPS(beta = beta,
      numIter = NUMITER,
      normalize = NORMALIZE,
      line_search = LINESEARCH,
      epsilon = EPSILON)
//    val model = fw.fit(columns, Y)

    val cols = env.fromCollection(columnGenerator(dimension, size))

    val alpha2 = env.fromCollection(sparseEntryGenerator(Array(1,2,3,4),Array(0.1,0.2,0.3,0.4)))

    val signal = signalGenerator(cols,0.0,alpha2)

    val model2 = fw.fit(cols, signal)
    // Sink
    env.fromElements(model2).print()
    env.execute()
  }

  //  def columnGenerator(dim:Int, num:Int): Set[ColumnVector] = {
  //    val s = (0 until num).map(i => ColumnVector(i,DenseVector.rand[Double](dim))).toSet
  //    s
  //  }

  def columnGenerator(dim: Int, num: Int): Stream[ColumnVector] = {
    val gen = for {i <- (0 until num).toStream} yield ColumnVector(i, normalize(DenseVector.rand[Double](dim)))
    gen
  }

  def signalGenerator(colVec: DataSet[ColumnVector], noise: Double, coeff: DataSet[SparseEntry]): DataSet[DenseVector[Double]] = {
    val joinedDataSet = colVec.joinWithTiny(coeff).where("idx").equalTo("index")
    val ret = joinedDataSet.map {
      x => x._1.values.copy *= x._2.value
//      new MyMap
    } reduce {
      (left, right) => left + right
    }
    ret.map {
      x => normalize (x+DenseVector.rand[Double](x.length)*noise)
    }
  }

  def sparseEntryGenerator(indices:Array[Int], coeff:Array[Double]): Array[SparseEntry] = {
    indices.zip(coeff).map(x => SparseEntry(x._1, x._2))
  }

  case class SparseEntry(index: Int, value: Double)

  class MyMap extends RichMapFunction[(ColumnVector, SparseEntry), DenseVector[Double]] {



    def write(filepath:String, data:List[String]): Unit = {

      val conf = ConfigFactory.load("job.conf")
      System.setProperty("HADOOP_USER_NAME", "hdfs")
      val path = new Path(filepath)
      val config = new Configuration()
      config.set("fs.defaultFS", conf.getString("hdfs.uri"))
      val fs = FileSystem.get(config)

      if(fs.exists(path)) {
        fs.delete(path, false)
      }
      val os = fs.create(path)
      data.foreach(a => os.write(a.getBytes()))

      fs.close()

      }

    override def map(value: (ColumnVector, SparseEntry)): DenseVector[Double] = {
//      println(getRuntimeContext.getIndexOfThisSubtask + " " + value)
      value._1.values.copy *= value._2.value

    }
  }

}