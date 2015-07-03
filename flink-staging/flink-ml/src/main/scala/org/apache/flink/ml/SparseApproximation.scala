package org.apache.flink.ml

import breeze.linalg.{VectorBuilder, SparseVector, DenseMatrix, DenseVector}

/**
 * Created by nltran on 01/07/15.
 */
@SerialVersionUID(123L)
object SparseApproximation extends Serializable {
  def initialApproximation: SparseApproximation = {
    new SparseApproximation(
      Array.empty[Array[Double]], Array.empty[Int], Array.empty[Double]
    )
  }
}

@SerialVersionUID(123L)
case class SparseApproximation(
                                atoms: Array[Array[Double]],
                                idx: Array[Int],
                                coef: Array[Double]) extends Serializable {
  def compute(): DenseVector[Double] = {
    if (!isEmpty) {
      new DenseMatrix[Double](atoms(0).length, atoms.length, atoms.flatten) *
        new DenseVector[Double](coef)
    }
    else {
      DenseVector.zeros(0)
    }
  }

  def isEmpty: Boolean = {
    atoms.length == 0
  }

  def toSparseVector(length: Int): SparseVector[Double] = {
    val builder = new VectorBuilder[Double](length)
    val tuples = idx zip coef
    for (t <- tuples) {
      builder.add(t._1, t._2)
    }
    builder.toSparseVector
  }
}