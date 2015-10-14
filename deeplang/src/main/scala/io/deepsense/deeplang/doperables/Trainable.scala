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

package io.deepsense.deeplang.doperables

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.Metadata

import io.deepsense.commons.types.ColumnType
import io.deepsense.commons.types.ColumnType.ColumnType
import io.deepsense.deeplang.doperables.Trainable.TrainingParameters
import io.deepsense.deeplang.doperables.dataframe.DataFrame
import io.deepsense.deeplang.doperables.dataframe.types.categorical.MappingMetadataConverter
import io.deepsense.deeplang.doperations.exceptions.WrongColumnTypeException
import io.deepsense.deeplang.inference.{InferContext, InferenceWarnings}
import io.deepsense.deeplang.{DKnowledge, DMethod1To1, DOperable, ExecutionContext}
import io.deepsense.entitystorage.UniqueFilenameUtil

trait Trainable extends DOperable {
  val train: DMethod1To1[TrainableParameters, DataFrame, Scorable] =
    new DMethod1To1[TrainableParameters, DataFrame, Scorable] {

      override def apply(context: ExecutionContext)
          (parameters: TrainableParameters)
          (dataFrame: DataFrame): Scorable = {

        val scorable = runTraining(context, parameters, dataFrame)(actualTraining)
        scorable
      }

      override def infer(
          context: InferContext)(
          parameters: TrainableParameters)(
          dataFrame: DKnowledge[DataFrame]): (DKnowledge[Scorable], InferenceWarnings) = {

        actualInference(context)(parameters)(dataFrame)
      }
    }

  protected type TrainScorable = TrainingParameters => Scorable

  protected type RunTraining = (
    ExecutionContext, TrainableParameters, DataFrame) => (TrainScorable => Scorable)

  /**
   * This is the main method of train()
   * It should be, if possible, overridden with one of the runTraining* methods
   * defined in Trainable trait. They provide a frame for execution of actual training.
   */
  protected def runTraining: RunTraining

  /**
   * This version of runTraining provides the training code with cached labeled points.
   * It un-caches them afterwards.
   */
  protected def runTrainingWithLabeledPoints: RunTraining =
    runTrainingWithLabeledPoints(caching = true, calculateNumClasses = false)

  /**
   * This version of runTraining provides the training code with NOT cached labeled points.
   */
  protected def runTrainingWithUncachedLabeledPoints: RunTraining =
    runTrainingWithLabeledPoints(caching = false, calculateNumClasses = false)

  /**
   * This version of runTraining is designed for classification
   * and provides the training code with cached labeled points.
   * It un-caches them afterwards.
   */
  protected def runClassificationTrainingWithLabeledPoints: RunTraining =
    runTrainingWithLabeledPoints(caching = true, calculateNumClasses = true)

  /**
   * This version of runTraining is designed for classification
   * and provides the training code with NOT cached labeled points.
   */
  protected def runClassificationTrainingWithUncachedLabeledPoints: RunTraining =
    runTrainingWithLabeledPoints(caching = false, calculateNumClasses = true)

  private def runTrainingWithLabeledPoints(
      caching: Boolean,
      calculateNumClasses: Boolean): RunTraining =
    (context, parameters, dataFrame) => {
      (trainScorable: TrainScorable) => {
        val (featureColumns, labelColumn) = parameters.columnNames(dataFrame)
        val labeledPoints = selectLabeledPointRDD(dataFrame, labelColumn, featureColumns)
        val numClasses =
          if (calculateNumClasses) Some(calculateNumberOfClasses(dataFrame, labelColumn)) else None
        val trainingParameters =
          TrainingParameters(dataFrame, labeledPoints, featureColumns, labelColumn, numClasses)
        def trainAction = () => trainScorable(trainingParameters)

        if (caching) {
          executeActionWithCaching[Scorable](labeledPoints, trainAction)
        } else {
          trainAction()
        }
      }
    }

  private def calculateNumberOfClasses(dataFrame: DataFrame, labelColumn: String): Int = {
    val columnType: ColumnType = dataFrame.columnType(labelColumn)
    val numClasses = columnType match {
      case ColumnType.boolean => 2
      case ColumnType.categorical =>
        val columnMetadata: Metadata = dataFrame.sparkDataFrame.schema(labelColumn).metadata
        val categoriesMapping = MappingMetadataConverter.mappingFromMetadata(columnMetadata)
        categoriesMapping.get.numberOfMappings
      case ColumnType.numeric =>
        val rdd: RDD[Double] = dataFrame.selectDoubleRDD(
          labelColumn,
          ColumnTypesPredicates.isNumeric)
        rdd.distinct().count().toInt
      case _ => throw new WrongColumnTypeException(
        s"Invalid label column type for classification: $columnType. Supported column types: " +
        s"[${ColumnType.boolean}, ${ColumnType.categorical}, ${ColumnType.numeric}}]")
    }
    if (numClasses < 2) {
      throw new WrongColumnTypeException(
        s"Selected label column: '$labelColumn' has $numClasses unique values. " +
        s"Column has to have >= 2 unique values to run classification.")
    }
    numClasses
  }

  private def executeActionWithCaching[T](rddToCache: RDD[LabeledPoint], action: () => T): T = {
    rddToCache.cache()
    val result = action()
    rddToCache.unpersist()
    result
  }

  /**
   * This method should be overridden with the actual execution of training.
   * It accepts [[TrainingParameters]] and returns a [[Scorable]] instance.
   */
  protected def actualTraining: TrainScorable

  protected def actualInference(
      context: InferContext)(
      parameters: TrainableParameters)(
      dataFrame: DKnowledge[DataFrame]): (DKnowledge[Scorable], InferenceWarnings)

  /**
   * The predicate that the label column has to meet.
   */
  protected def labelPredicate: ColumnTypesPredicates.Predicate

  /**
   * The predicate all feature columns have to meet.
   */
  protected def featurePredicate: ColumnTypesPredicates.Predicate

  private def selectLabeledPointRDD(
      dataFrame: DataFrame, target: String, features: Seq[String]): RDD[LabeledPoint] = {

    dataFrame.selectAsSparkLabeledPointRDD(
      target, features, labelPredicate = labelPredicate, featurePredicate = featurePredicate)
  }

  protected def saveScorable(context: ExecutionContext, scorable: Scorable): Unit = {
    val uniquePath = context.uniqueFsFileName(UniqueFilenameUtil.ModelEntityCategory)
    scorable.save(context)(uniquePath)
  }
}

object Trainable {
  case class TrainingParameters(
    dataFrame: DataFrame,
    labeledPoints: RDD[LabeledPoint],
    features: Seq[String],
    target: String,
    numberOfClasses: Option[Int] = None)
}
