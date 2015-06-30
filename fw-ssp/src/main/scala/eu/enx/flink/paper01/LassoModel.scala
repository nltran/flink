package eu.enx.flink.paper01

/**
 * Created by Thomas Peel @ Eura Nova
 * on 30/06/15.
 */
// Case classes
case class LassoModel(var index: Array[Int], var data: Array[Double]) extends Serializable {
  override def toString: String = {
    if (index == null) "Empty model."
    else {
      assert(index.length == data.length)
      index.zip(data).map {
        case (a: Int, b: Double) => a + "," + b
      }.mkString("\n")
    }
  }
}
