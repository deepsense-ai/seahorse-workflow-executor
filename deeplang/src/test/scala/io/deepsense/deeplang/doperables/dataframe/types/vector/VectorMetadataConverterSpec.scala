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

import org.apache.spark.sql.types.MetadataBuilder

import io.deepsense.deeplang.UnitSpec

class VectorMetadataConverterSpec extends UnitSpec {

  val vectorMetadata = VectorMetadata(3467547700454L)

  "VectorMetadataConverterSpec" should {
    "convert spark's metadata to VectorMetadata and the other way around" in {
      val oldSparkMetadata = new MetadataBuilder().build()
      val sparkMetadata = VectorMetadataConverter.toSchemaMetadata(vectorMetadata, oldSparkMetadata)
      val readMetadata = VectorMetadataConverter.fromSchemaMetadata(sparkMetadata)
      readMetadata.get shouldBe vectorMetadata
    }
    "convert VectorMetadata to spark's metadata preserving old metadata" in {
      val oldSparkMetadata = new MetadataBuilder().putDouble("X", 3.14).build()
      val sparkMetadata = VectorMetadataConverter.toSchemaMetadata(vectorMetadata, oldSparkMetadata)
      sparkMetadata.getDouble("X") shouldBe 3.14
    }
  }
}
