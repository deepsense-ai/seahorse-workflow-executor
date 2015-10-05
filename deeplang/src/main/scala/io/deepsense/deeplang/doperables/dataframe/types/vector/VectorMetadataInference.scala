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

package io.deepsense.deeplang.doperables.dataframe.types.vector

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.types.{StructField, StructType}

import io.deepsense.commons.types.ColumnType
import io.deepsense.deeplang.ExecutionContext
import io.deepsense.deeplang.doperables.dataframe.DataFrame
import io.deepsense.deeplang.doperables.dataframe.types.SparkConversions
import io.deepsense.deeplang.doperations.exceptions.SchemaMismatchException

trait VectorMetadataInference {

  def inferVectorMetadata(context: ExecutionContext, dataFrame: DataFrame): DataFrame = {
    val vectorLengths = dataFrame.sparkDataFrame.map(_.toSeq.map {
      case DenseVector(values) => Some(values.length)
      case SparseVector(size, _, _) => Some(size)
      case _ => None
    })

    val vectorColumnLengths = vectorLengths.reduce {
      case (r1, r2) =>
        r1.zip(r2).map {
          case (None, None) => None
          case (Some(v), None) => Some(v)
          case (None, Some(v)) => Some(v)
          case (Some(v1), Some(v2)) if v1 == v2 => Some(v1)
          case _ => throw SchemaMismatchException("column contains vectors of different lengths")
        }
    }

    val convertedSchema = StructType(dataFrame.sparkDataFrame.schema.zip(vectorColumnLengths).map {
      case (field@StructField(_, dataType, _, _), Some(length))
        if SparkConversions.sparkColumnTypeToColumnType(dataType) == ColumnType.vector =>
        field.copy(
          metadata = VectorColumnMetadata(length).toSparkMetadata())
      case (field, _) => field
    })

    context.dataFrameBuilder.buildDataFrame(convertedSchema, dataFrame.sparkDataFrame.rdd)
  }
}
