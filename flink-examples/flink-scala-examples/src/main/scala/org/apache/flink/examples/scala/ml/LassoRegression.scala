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

package org.apache.flink.examples.scala.ml

import breeze.linalg._
import breeze.numerics._
import org.apache.flink.api.scala._
import org.apache.flink.ml.regression.{LassoWithPS, ColumnVector, Lasso}

/**
 * This example implements a basic Lasso Regression with a distributed Franck-Wolfe optimization method.
 *
 * Created by Thomas Peel @ Eura Nova
 * on 18/05/15.
 */

object LassoRegression {
  def main(args: Array[String]) {
    val EPSILON = 1e-3
    val PARALLELISM = 1
    val NUMITER = 10
    val NORMALIZE = false
    val LINESEARCH = true
    val NOISE_LEVEL = 0.0
    val OPT = "GR"

    val dimension = 512
    val size = dimension

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(PARALLELISM)

//    val X: DenseMatrix[Double] = DenseMatrix.eye[Double](dimension)
    val X: DenseMatrix[Double] = DenseMatrix.rand[Double](dimension, size)
    val alpha = SparseVector.zeros[Double](size)
    alpha.update(1, 0.45)
    alpha.update(2, 0.18)
    alpha.update(3, 0.98)
    alpha.update(4, 0.67)

    val beta = sum(abs(alpha))

    val noise = DenseVector.rand[Double](dimension) *= NOISE_LEVEL

    val Y = env.fromElements((X * alpha + noise).toArray)

    val columns = env.fromCollection({
      0 until size
    } map { i => ColumnVector(i, X(::, i).toArray) })

    val fw = new Lasso(beta = beta,
      numIter = NUMITER,
      normalize = NORMALIZE,
      line_search = LINESEARCH,
      epsilon = EPSILON)

    val model = fw.fit(columns, Y, log = true)

    // Sink
    env.fromElements(model).print()
//    println(env.getExecutionPlan())
    env.execute()
  }
}