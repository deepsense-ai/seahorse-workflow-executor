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

package io.deepsense.deeplang.doperables.dataframe.types.categorical

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

import io.deepsense.commons.types.ColumnType
import io.deepsense.commons.types.ColumnType._
import io.deepsense.deeplang.doperables.dataframe.types.{ColumnMetadataBuilder, NonEmptyColumnMetadata}

/**
 * Represents knowledge about categorical column.
 * @param categories Mapping of categories in this column.
 */
case class CategoricalColumnMetadata(
    categories: CategoriesMapping)
  extends NonEmptyColumnMetadata {

  val columnType: ColumnType = ColumnType.categorical

  protected def toSparkInnerMetadata: Metadata = {
    val mappingMetadataBuilder = new MetadataBuilder()
    val mappingMetadata = categories.values
      .foldLeft(mappingMetadataBuilder) { case (b, v) => b.putLong(v, categories.valueToId(v))}
      .build()
    new MetadataBuilder()
      .putStringArray(CategoricalColumnMetadataBuilder.ValuesKey, categories.values.toArray)
      .putMetadata(CategoricalColumnMetadataBuilder.MappingKey, mappingMetadata)
      .build()
  }
}

object CategoricalColumnMetadata {
  def apply(categoriesNames: Seq[String]): CategoricalColumnMetadata =
    CategoricalColumnMetadata(CategoriesMapping(categoriesNames))

  def apply(categoriesNames: String*)(implicit d: DummyImplicit): CategoricalColumnMetadata =
    CategoricalColumnMetadata(categoriesNames)
}

object CategoricalColumnMetadataBuilder
  extends ColumnMetadataBuilder[CategoricalColumnMetadata] {
  val columnType = ColumnType.categorical

  val ValuesKey = "values"
  val MappingKey = "mapping"

  protected def fromInnerSparkMetadata(metadata: Metadata): CategoricalColumnMetadata = {
    val values = metadata.getStringArray(ValuesKey).toSet
    val mapping = metadata.getMetadata(MappingKey)
    val valueIdPairs = values.map(v => v -> mapping.getLong(v).toInt)
    val valueToId = valueIdPairs.toMap
    val idToValue = valueIdPairs.map { case (v, id) => id -> v }.toMap
    CategoricalColumnMetadata(CategoriesMapping(valueToId, idToValue))
  }
}
