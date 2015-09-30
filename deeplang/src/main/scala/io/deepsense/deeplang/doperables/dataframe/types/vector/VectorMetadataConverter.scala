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

import scala.util.Try

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

trait VectorMetadataConverter {

  /**
   * Vector metadata (if any) from Spark's StructField metadata.
   */
  def fromSchemaMetadata(metadata: Metadata): Option[VectorMetadata] =
    Try {
      val vectorMetadata = metadata.getMetadata(VectorMetadataConverter.VectorKey)
      val length = vectorMetadata.getLong(VectorMetadataConverter.LengthKey)
      VectorMetadata(length)
    }.toOption

  /**
   * Vector metadata as Spark's StructField metadata.
   * Persists all data stored under different key.
   */
  def toSchemaMetadata(vectorMetadata: VectorMetadata, metadata: Metadata): Metadata =
    new MetadataBuilder()
      .withMetadata(metadata)
      .putMetadata(VectorMetadataConverter.VectorKey, toFlatSchemaMetadata(vectorMetadata))
      .build()

  /**
   * Vector metadata as Spark's StructField metadata.
   */
  def toSchemaMetadata(vectorMetadata: VectorMetadata): Metadata =
    new MetadataBuilder()
      .putMetadata(VectorMetadataConverter.VectorKey, toFlatSchemaMetadata(vectorMetadata))
      .build()

  private def toFlatSchemaMetadata(vectorMetadata: VectorMetadata): Metadata = {
    new MetadataBuilder()
      .putLong(VectorMetadataConverter.LengthKey, vectorMetadata.length)
      .build()
  }
}

object VectorMetadataConverter extends VectorMetadataConverter {
  val VectorKey = "vector"
  val LengthKey = "length"
}
