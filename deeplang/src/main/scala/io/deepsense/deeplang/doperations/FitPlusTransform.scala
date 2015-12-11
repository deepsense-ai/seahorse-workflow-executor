/**
 * Copyright 2015, deepsense.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.deepsense.deeplang.doperations

import scala.reflect.runtime.universe._

import spray.json.{JsNull, JsValue}

import io.deepsense.deeplang.DOperation.Id
import io.deepsense.deeplang.doperables.dataframe.DataFrame
import io.deepsense.deeplang.doperables.{Estimator, Transformer}
import io.deepsense.deeplang.doperations.exceptions.TooManyPossibleTypesException
import io.deepsense.deeplang.inference.{InferContext, InferenceWarnings}
import io.deepsense.deeplang.params.{DynamicParam, Param}
import io.deepsense.deeplang.{DKnowledge, DOperation2To2, ExecutionContext}

class FitPlusTransform extends DOperation2To2[Estimator, DataFrame, DataFrame, Transformer] {

  override val id: Id = "1cb153f1-3731-4046-a29b-5ad64fde093f"
  override val name: String = "Fit + Transform"
  override val description: String = "Fits an Estimator on a DataFrame and transform it"
  override val tTagTO_0: TypeTag[DataFrame] = typeTag[DataFrame]
  override val tTagTO_1: TypeTag[Transformer] = typeTag[Transformer]
  override val tTagTI_0: TypeTag[Estimator] = typeTag[Estimator]
  override val tTagTI_1: TypeTag[DataFrame] = typeTag[DataFrame]

  val estimatorParams = new DynamicParam(
    name = "Parameters of input Estimator",
    description = "These parameters are rendered dynamically, depending on type of Estimator",
    inputPort = 0)
  setDefault(estimatorParams -> JsNull)

  def setEstimatorParams(jsValue: JsValue): this.type = set(estimatorParams -> jsValue)
  override val params: Array[Param[_]] = declareParams(estimatorParams)

  override protected def _execute(
      context: ExecutionContext)(
      estimator: Estimator,
      dataFrame: DataFrame): (DataFrame, Transformer) = {
    val estimatorToRun = estimatorWithParams(estimator)
    val transformer: Transformer = estimatorToRun.fit(context)(())(dataFrame)
    val transformed: DataFrame = transformer.transform(context)(())(dataFrame)
    (transformed, transformer)
  }

  override protected def _inferKnowledge(
      context: InferContext)(
      estimatorKnowledge: DKnowledge[Estimator],
      inputDataFrameKnowledge: DKnowledge[DataFrame])
      : ((DKnowledge[DataFrame], DKnowledge[Transformer]), InferenceWarnings) = {

    val (transformerKnowledge, transformerWarnings) =
      inferTransformer(context, estimatorKnowledge, inputDataFrameKnowledge)

    val (transformedDataFrameKnowledge, transformedDataFrameWarnings) =
      inferDataFrame(context, inputDataFrameKnowledge, transformerKnowledge)

    val warningsSum: InferenceWarnings = transformerWarnings ++ transformedDataFrameWarnings
    ((transformedDataFrameKnowledge, transformerKnowledge), warningsSum)
  }

  private def estimatorWithParams(estimator: Estimator): Estimator = {
    estimator.replicate().setParamsFromJson($(estimatorParams))
  }

  private def inferDataFrame(
      context: InferContext,
      inputDataFrameKnowledge: DKnowledge[DataFrame],
      transformerKnowledge: DKnowledge[Transformer])
      : (DKnowledge[DataFrame], InferenceWarnings) = {
    val (transformedDataFrameKnowledge, transformedDataFrameWarnings) =
      transformerKnowledge.single.transform.infer(context)(())(inputDataFrameKnowledge)
    (transformedDataFrameKnowledge, transformedDataFrameWarnings)
  }

  private def inferTransformer(
      context: InferContext,
      estimatorKnowledge: DKnowledge[Estimator],
      inputDataFrameKnowledge: DKnowledge[DataFrame])
      : (DKnowledge[Transformer], InferenceWarnings) = {
    throwIfToManyTypes(estimatorKnowledge)
    val estimator: Estimator = estimatorWithParams(estimatorKnowledge.single)
    val (transformerKnowledge, transformerWarnings) =
      estimator.fit.infer(context)(())(inputDataFrameKnowledge)
    (transformerKnowledge, transformerWarnings)
  }

  private def throwIfToManyTypes(estimatorKnowledge: DKnowledge[_]): Unit = {
    if (estimatorKnowledge.types.size > 1) {
      throw TooManyPossibleTypesException()
    }
  }
}
