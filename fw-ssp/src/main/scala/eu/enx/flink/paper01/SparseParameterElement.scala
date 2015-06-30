package eu.enx.flink.paper01

import org.apache.flink.ps.model.ParameterElement

/**
 * Created by Thomas Peel @ Eura Nova
 * on 30/06/15.
 */

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

object SparseParameterElement {
  def empty: SparseParameterElement = {
    new SparseParameterElement
  }
}
