package org.apache.flink.ml

import org.apache.flink.ps.model.ParameterElement

/**
 * Created by Thomas Peel @ Eura Nova
 * on 3/07/15.
 */

class SparseCoefficientElement (
    clock: Int = 0,
    value: (Array[Int], Array[Double]) = (Array.empty[Int], Array.empty[Double])
  ) extends ParameterElement[(Array[Int], Array[Double])] {

  def getClock: Int = clock
  def getValue: (Array[Int], Array[Double]) = value
}