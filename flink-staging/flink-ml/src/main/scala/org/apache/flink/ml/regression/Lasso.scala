package org.apache.flink.ml.regression

import org.apache.flink.api.common.functions.RichMapFunctionWithSSPServer
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{ParameterMap, Learner}

/**
 * Created by Thomas Peel @ Eura Nova
 * on 18/05/15.
 */
class Lasso extends Learner [LargeScaleRegressionDataset, LassoModel]{
  def fit(input: DataSet[LargeScaleRegressionDataset], fitParameters: ParameterMap): LassoModel = {
    new LassoModel()
  }
}

private case class LargeScaleRegressionDataset(entries: Set[(Int, Int, Double)], targets: Array[Double])
private class LassoModel {
  private var coef_ = 0.0
}

private class MyMapFucntion extends RichMapFunctionWithSSPServer[] {

}