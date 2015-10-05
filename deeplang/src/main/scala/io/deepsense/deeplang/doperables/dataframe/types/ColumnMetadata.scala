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

package io.deepsense.deeplang.doperables.dataframe.types

import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}

import io.deepsense.commons.types.ColumnType._
import io.deepsense.deeplang.doperables.dataframe.types.categorical.CategoricalColumnMetadataBuilder
import io.deepsense.deeplang.doperables.dataframe.types.vector.VectorColumnMetadataBuilder

sealed trait ColumnMetadata {
  private[dataframe] def toSparkMetadata(oldMetadata: Metadata = Metadata.empty): Metadata
}

object ColumnMetadata {
  val DsMetadataKey = "deepsense"

  def keyFromColumnType(columnType: ColumnType): String = columnType.toString

  private val builders: Seq[ColumnMetadataBuilder[ColumnMetadata]] = Seq(
    VectorColumnMetadataBuilder,
    CategoricalColumnMetadataBuilder)

  private val buildersMap = builders.map { b => b.columnType -> b }.toMap

  def fromStructField(structField: StructField): ColumnMetadata = {
    val columnType = SparkConversions.sparkColumnTypeToColumnType(structField.dataType)
    buildersMap.get(columnType) match {
      case Some(builder) =>
        val sparkMetadata = structField.metadata
        require(sparkMetadata.contains(DsMetadataKey),
          s"Field of type '$columnType' (corresponding to spark's '${structField.dataType}')" +
            s"should contain deepsense metadata")
        builder.fromSparkMetadata(sparkMetadata)
      case None => EmptyColumnMetadata
    }
  }
}

object EmptyColumnMetadata extends ColumnMetadata {
  private[dataframe] def toSparkMetadata(oldMetadata: Metadata): Metadata = oldMetadata
}

abstract class NonEmptyColumnMetadata extends ColumnMetadata {
  def columnType: ColumnType

  protected lazy val key = ColumnMetadata.keyFromColumnType(columnType)

  def toSparkMetadata(oldMetadata: Metadata = Metadata.empty): Metadata = {
    val innerMetadata = toSparkInnerMetadata
    val dsMetadata = new MetadataBuilder()
      .putMetadata(key, innerMetadata)
      .build()
    new MetadataBuilder()
      .withMetadata(oldMetadata)
      .putMetadata(ColumnMetadata.DsMetadataKey, dsMetadata)
      .build()
  }

  protected def toSparkInnerMetadata: Metadata
}

// TODO sealed
trait ColumnMetadataBuilder[+T <: ColumnMetadata] {
  def columnType: ColumnType

  protected lazy val key = ColumnMetadata.keyFromColumnType(columnType)

  def tryFromSparkMetadata(metadata: Metadata): Option[T] = {
    if (metadata.contains(ColumnMetadata.DsMetadataKey)) {
      val unwrappedMetadata = metadata.getMetadata(ColumnMetadata.DsMetadataKey)
      if (unwrappedMetadata.contains(key)) {
        val innerMetadata = unwrappedMetadata.getMetadata(key)
        Some(fromInnerSparkMetadata(innerMetadata))
      } else {
        None
      }
    } else {
      None
    }
  }

  def fromSparkMetadata(metadata: Metadata): T = tryFromSparkMetadata(metadata).get

  protected def fromInnerSparkMetadata(metadata: Metadata): T
}
