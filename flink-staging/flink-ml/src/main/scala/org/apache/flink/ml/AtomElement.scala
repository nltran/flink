package org.apache.flink.ml

import org.apache.flink.ps.model.ParameterElement

/**
 * Created by Thomas Peel @ Eura Nova
 * on 3/07/15.
 */
class AtomElement (
    clock: Int = 0,
    value: Array[Double] = Array.empty[Double]
  ) extends ParameterElement[Array[Double]] {

  def getClock: Int = clock
  def getValue: Array[Double] = value
}
