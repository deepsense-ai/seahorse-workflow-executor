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

import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types.{IntegerType, Metadata, MetadataBuilder, StringType, StructField}

import io.deepsense.deeplang.UnitSpec
import io.deepsense.deeplang.doperables.dataframe.types.categorical.{CategoricalColumnMetadataBuilder, CategoricalColumnMetadata, CategoriesMapping}
import io.deepsense.deeplang.doperables.dataframe.types.vector.{VectorColumnMetadataBuilder, VectorColumnMetadata}

class ColumnMetadataSpec extends UnitSpec {

  "ColumnMetadata" should {
    "convert to and from spark metadata preserving previous value" when {
      "contains empty metadata" in {
        val sparkMetadata = EmptyColumnMetadata.toSparkMetadata(oldSparkMetadata)
        val structField = StructField("name", StringType, metadata = sparkMetadata)
        val readMetadata = ColumnMetadata.fromStructField(structField)
        readMetadata shouldBe EmptyColumnMetadata

        verifySparkMetadataPreserved(sparkMetadata)
      }
      "contains categorical metadata" in {
        val sparkMetadata = categoricalMetadata.toSparkMetadata(oldSparkMetadata)
        val structField = StructField("name", IntegerType, metadata = sparkMetadata)
        val readMetadata = ColumnMetadata.fromStructField(structField)
        readMetadata shouldBe categoricalMetadata

        verifySparkMetadataPreserved(sparkMetadata)
      }
      "contains vector metadata" in {
        val sparkMetadata = vectorMetadata.toSparkMetadata(oldSparkMetadata)
        val structField = StructField("name", new VectorUDT(), metadata = sparkMetadata)
        val readMetadata = ColumnMetadata.fromStructField(structField)
        readMetadata shouldBe vectorMetadata

        verifySparkMetadataPreserved(sparkMetadata)
      }
    }
  }

  "CategoricalColumnMetadataBuilder" should {
    "return None" when {
      "extracting metadata from non categorical field" in {
        val sparkMetadata = vectorMetadata.toSparkMetadata()
        CategoricalColumnMetadataBuilder.tryFromSparkMetadata(sparkMetadata) shouldBe None
      }
      "extracting from field with empty metadata" in {
        CategoricalColumnMetadataBuilder.tryFromSparkMetadata(Metadata.empty) shouldBe None
      }
    }
    "return Some column metadata" when {
      "extracting from categorical field" in {
        val sparkMetadata = categoricalMetadata.toSparkMetadata()
        val extractedMetadata = CategoricalColumnMetadataBuilder.tryFromSparkMetadata(sparkMetadata)
        extractedMetadata shouldBe Some(categoricalMetadata)
      }
    }
  }

  "VectorColumnMetadataBuilder" should {
    "return None" when {
      "extracting metadata from non vector field" in {
        val sparkMetadata = categoricalMetadata.toSparkMetadata()
        VectorColumnMetadataBuilder.tryFromSparkMetadata(sparkMetadata) shouldBe None
      }
      "extracting from field with empty metadata" in {
        VectorColumnMetadataBuilder.tryFromSparkMetadata(Metadata.empty) shouldBe None
      }
    }
    "return Some column metadata" when {
      "extracting from vector field" in {
        val sparkMetadata = vectorMetadata.toSparkMetadata()
        val extractedMetadata = VectorColumnMetadataBuilder.tryFromSparkMetadata(sparkMetadata)
        extractedMetadata shouldBe Some(vectorMetadata)
      }
    }
  }

  private def oldSparkMetadata: Metadata = new MetadataBuilder().putDouble("X", 3.14).build()

  private def verifySparkMetadataPreserved(newMetadata: Metadata): Unit = {
    newMetadata.getDouble("X") shouldBe 3.14
  }

  private def categoricalMetadata = {
    val idToValue = Map(2 -> "two", 20 -> "twenty", 1337 -> "categories")
    val valueToId = idToValue.map(_.swap)
    val mapping = CategoriesMapping(valueToId, idToValue)
    CategoricalColumnMetadata(mapping)
  }

  private def vectorMetadata = VectorColumnMetadata(3467547700454L)
}
