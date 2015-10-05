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

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

import io.deepsense.commons.types.ColumnType
import io.deepsense.commons.types.ColumnType._
import io.deepsense.deeplang.doperables.dataframe.types.{ColumnMetadataBuilder, NonEmptyColumnMetadata}

/**
 * Represents knowledge about fixed-size vector column.
 * @param length Length of each vector in this column.
 */
case class VectorColumnMetadata(length: Long) extends NonEmptyColumnMetadata {
  def columnType: ColumnType = ColumnType.vector

  protected def toSparkInnerMetadata: Metadata = {
    new MetadataBuilder()
      .putLong(VectorColumnMetadataBuilder.lengthKey, length)
      .build()
  }
}

object VectorColumnMetadataBuilder
  extends ColumnMetadataBuilder[VectorColumnMetadata] {

  val columnType = ColumnType.vector

  val lengthKey = "length"

  protected def fromInnerSparkMetadata(metadata: Metadata): VectorColumnMetadata = {
    val length = metadata.getLong(lengthKey)
    VectorColumnMetadata(length)
  }
}
