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

import org.apache.spark.sql.types._

import io.deepsense.commons.types.ColumnType
import io.deepsense.deeplang.catalogs.doperable.DOperableCatalog
import io.deepsense.deeplang.doperables.dataframe._
import io.deepsense.deeplang.doperables.dataframe.types.categorical.{CategoricalColumnMetadata, CategoriesMapping}
import io.deepsense.deeplang.inference.{ConversionMayNotBePossibleWarning, InferContext}
import io.deepsense.deeplang.{DKnowledge, UnitSpec}

class ConvertTypeSpec extends UnitSpec {

  "ConvertType" should {
    "return properly converted metadata without warnings" in {
      val df = DataFrameBuilder.buildDataFrameForInference(metadata)
      val inferContext = InferContext(
        mock[DOperableCatalog],
        fullInference = true)

      val (knowledge, warnings) = ConvertType(
        ColumnType.categorical,
        Set(stringColumn.name), Set())
        .inferKnowledge(inferContext)(Vector(new DKnowledge[DataFrame](df)))

      warnings shouldBe empty
      knowledge should have size 1
      knowledge(0).types should have size 1

      val inferredMetadata =
        knowledge(0).types.head.inferredMetadata.get.asInstanceOf[DataFrameMetadata]

      inferredMetadata.orderedColumns shouldBe Seq(
        numericColumn,
        categoricalColumn1,
        ColumnKnowledge.categorical(stringColumn.name, stringColumn.index, None),
        categoricalColumn2
      )
    }

    "produce warning when type may not be convertible" in {
      val df = DataFrameBuilder.buildDataFrameForInference(metadata)
      val inferContext = InferContext(
        mock[DOperableCatalog],
        fullInference = true)

      val (knowledge, warnings) = ConvertType(
        ColumnType.numeric,
        Set(stringColumn.name),
        Set())
        .inferKnowledge(inferContext)(Vector(new DKnowledge[DataFrame](df)))

      warnings.warnings should have size 1
      warnings.warnings.head shouldBe ConversionMayNotBePossibleWarning(
        stringColumn,
        ColumnType.numeric)

      knowledge should have size 1
      knowledge(0).types should have size 1

      val inferredMetadata =
        knowledge(0).types.head.inferredMetadata.get.asInstanceOf[DataFrameMetadata]

      inferredMetadata.orderedColumns shouldBe Seq(
        numericColumn,
        categoricalColumn1,
        ColumnKnowledge(stringColumn.name, stringColumn.index, numericColumn.columnType),
        categoricalColumn2
      )
    }

    "return properly converted metadata when run on fuzzy metadata warnings" in {

      val df = DataFrameBuilder.buildDataFrameForInference(fuzzyMetadata)
      val inferContext = InferContext(
        mock[DOperableCatalog],
        fullInference = true)

      val (knowledge, warnings) = ConvertType(ColumnType.categorical,
        // numericColumn does not exist, stringColumn does
        Set(numericColumn.name, stringColumn.name),
        // numericColumn does not exist, columnWithUnknownType does
        Set(numericColumn.index.get, columnWithUnknownType.index.get))
        .inferKnowledge(inferContext)(Vector(new DKnowledge[DataFrame](df)))

      // 2 warnings about selection problems
      // 1 warning about columnWithUnknownType may not be convertible
      warnings.warnings should have size 3
      knowledge should have size 1
      knowledge(0).types should have size 1

      val inferredMetadata =
        knowledge(0).types.head.inferredMetadata.get.asInstanceOf[DataFrameMetadata]

      inferredMetadata.orderedColumns shouldBe Seq(
        categoricalColumn1,
        ColumnKnowledge.categorical(stringColumn.name, stringColumn.index, None),
        categoricalColumnWithUnknownCategories,
        ColumnKnowledge.categorical(
          columnWithUnknownType.name,
          columnWithUnknownType.index,
          None),
        numericColumnWithUnknownIndex,
        categoricalColumnWithUnknownIndex
        )
    }
  }

  val mappings = List(
    CategoriesMapping(Seq("A", "B", "C")),
    CategoriesMapping(Seq("cat", "dog"))
  )

  val numericColumn = ColumnKnowledge(
    name = "num_column", index = Some(0), columnType = Some(ColumnType.numeric))

  val categoricalColumn1 = ColumnKnowledge.categorical(
    name = "categorical_1", index = Some(1), mappings(0))

  val stringColumn = ColumnKnowledge(
    name = "string_column", index = Some(2), columnType = Some(ColumnType.string))

  val categoricalColumn2 = ColumnKnowledge.categorical(
    name = "categorical_2", index = Some(3), mappings(1))

  val numericColumnWithUnknownIndex = ColumnKnowledge(
    name = "num_unknown_index", index = None, columnType = Some(ColumnType.numeric))

  val categoricalColumnWithUnknownIndex = ColumnKnowledge.categorical(
    name = "categorical_unknown_index", index = None, mappings(1))

  val categoricalColumnWithUnknownCategories = ColumnKnowledge.categorical(
    name = "categorical_unknown_categories", index = Some(4), None)

  val columnWithUnknownType = ColumnKnowledge(
    name = "unknown_type", index = Some(5), columnType = None)


  val schema = StructType(Seq(
    StructField(
      numericColumn.name,
      DoubleType),
    StructField(
      categoricalColumn1.name,
      IntegerType,
      metadata = CategoricalColumnMetadata(mappings(0)).toSparkMetadata()),
    StructField(
      stringColumn.name,
      StringType),
    StructField(
      categoricalColumn2.name,
      IntegerType,
      metadata = CategoricalColumnMetadata(mappings(1)).toSparkMetadata())
  ))

  val metadata = DataFrameMetadata(
    isExact = true,
    isColumnCountExact = true,
    columns = DataFrameMetadata.buildColumnsMap(Seq(
      numericColumn,
      categoricalColumn1,
      stringColumn,
      categoricalColumn2)
    )
  )

  val fuzzyMetadata = DataFrameMetadata(
    isExact = false,
    isColumnCountExact = false,
    columns = DataFrameMetadata.buildColumnsMap(Seq(
      categoricalColumn1,
      stringColumn,
      numericColumnWithUnknownIndex,
      categoricalColumnWithUnknownIndex,
      categoricalColumnWithUnknownCategories,
      columnWithUnknownType
    )))
}
