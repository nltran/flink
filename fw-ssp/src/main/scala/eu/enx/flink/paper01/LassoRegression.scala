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
import breeze.stats.distributions.Gaussian
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.ml.regression.{Lasso, ColumnVector, LassoWithPS}


/**
 * This example implements a basic Lasso Regression with a distributed Franck-Wolfe optimization
 * method.
 *
 * Created by Thomas Peel @ Eura Nova
 * on 18/05/15.
 */

object LassoRegression {
  def main(args: Array[String]) {
    val EPSILON = 1e-3
    val PARALLELISM = 3
    val NUMITER = 100
    val NORMALIZE = false
    val LINESEARCH = true
    val NOISE = 0.0
    val OPT = "GR"

    val dimension = 128
    val size = 1024

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(PARALLELISM)

    val beta = 1.0

    val fw = new Lasso(
      beta = beta,
      numIter = NUMITER,
      normalize = NORMALIZE,
      line_search = LINESEARCH,
      epsilon = EPSILON,
      opt = OPT)

    val cols = env.fromCollection(columnGenerator(dimension, size, "gaussian"))
    val alpha = env.fromCollection(
      sparseEntryGenerator(Array(1, 2, 3, 4), Array(0.1, 0.2, 0.3, 0.4))
    )

    val signal = signalGenerator(cols, NOISE, alpha)
    val model = fw.fit(cols, signal)

    // Sink
    env.fromElements(model).first(1).print()
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
      x => breeze.linalg.normalize(DenseVector(x) + DenseVector.rand[Double](x.length) *= noise)
        .toArray
    }
  }

  def sparseEntryGenerator(indices: Array[Int], coeff: Array[Double]): Array[SparseEntry] = {
    indices.zip(coeff).map(x => SparseEntry(x._1, x._2))
  }

  case class SparseEntry(index: Int, value: Double)

  class ElementWiseMul extends RichMapFunction[(ColumnVector, SparseEntry), Array[Double]] {
    override def map(value: (ColumnVector, SparseEntry)): Array[Double] = {
      (DenseVector(value._1.values) *= value._2.value).toArray
    }
  }

}