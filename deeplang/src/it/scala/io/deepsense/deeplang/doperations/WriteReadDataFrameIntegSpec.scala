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

import java.sql.Timestamp

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{Metadata, StructField, StructType}
import org.scalatest.BeforeAndAfter

import io.deepsense.commons.types.ColumnType
import io.deepsense.deeplang.DeeplangIntegTestSupport
import io.deepsense.deeplang.doperables.dataframe.DataFrame
import io.deepsense.deeplang.doperables.dataframe.types.SparkConversions
import io.deepsense.deeplang.doperables.dataframe.types.categorical.{CategoriesMapping, MappingMetadataConverter}
import io.deepsense.deeplang.doperables.dataframe.types.vector.{VectorMetadata, VectorMetadataConverter}
import io.deepsense.deeplang.doperations.CsvParameters.ColumnSeparator
import io.deepsense.deeplang.parameters._

class WriteReadDataFrameIntegSpec
  extends DeeplangIntegTestSupport
  with BeforeAndAfter {

  val absoluteWriteReadDataFrameTestPath =  absoluteTestsDirPath + "/WriteReadDataFrameTest"

  val time = System.currentTimeMillis()
  val timestamp = new Timestamp(time)

  val schema: StructType = StructType(Seq(
    StructField("boolean",
      SparkConversions.columnTypeToSparkColumnType(ColumnType.boolean)),
    StructField("categorical",
      SparkConversions.columnTypeToSparkColumnType(ColumnType.categorical),
      metadata = MappingMetadataConverter.mappingToMetadata(CategoriesMapping(Seq("A", "B", "C")))),
    StructField("numeric",
      SparkConversions.columnTypeToSparkColumnType(ColumnType.numeric)),
    StructField("string",
      SparkConversions.columnTypeToSparkColumnType(ColumnType.string)),
    StructField("timestamp",
      SparkConversions.columnTypeToSparkColumnType(ColumnType.timestamp)),
    StructField("vector",
      SparkConversions.columnTypeToSparkColumnType(ColumnType.vector),
      metadata = VectorMetadataConverter.toSchemaMetadata(VectorMetadata(3)))
  ))

  val rows = Seq(
    Row(true, 0, 0.45, "3.14", timestamp, Vectors.dense(1.0, 2.0, 3.0)),
    Row(false, 1, null, "\"testing...\"", null, Vectors.sparse(3, Seq((1, 4.0)))),
    Row(false, 2, 3.14159, "Hello, world!", timestamp, Vectors.dense(0.1, 0.2, 0.3)),
    Row(null, null, null, null, null, null)
  )

  val dataFrame = createDataFrame(rows, schema)

  before {
    fileSystemClient.delete(testsDir)
  }

  "WriteDataFrame and ReadDataFrame" should {
    "write and read PARQUET file" in {
      val wdf = WriteDataFrame(
        FileFormat.PARQUET,
        absoluteWriteReadDataFrameTestPath + "/parquet")
      wdf.execute(executionContext)(Vector(dataFrame))

      val rdf = ReadDataFrame(
        absoluteWriteReadDataFrameTestPath + "/parquet",
        FileFormat.PARQUET)
      val loadedDataFrame = rdf.execute(executionContext)(Vector()).head.asInstanceOf[DataFrame]

      assertDataFramesEqual(loadedDataFrame, dataFrame, checkRowOrder = false)
    }

    "insert vector metadata when reading PARQUET file" in {
      val convertedSchema = schema.copy()
      val vectorFieldIndex = convertedSchema.fieldIndex("vector")
      convertedSchema.fields.update(
        vectorFieldIndex,
        convertedSchema.fields(vectorFieldIndex).copy(metadata = Metadata.empty)
      )

      val convertedDataFrame = createDataFrame(rows, convertedSchema)

      val wdf = WriteDataFrame(
        FileFormat.PARQUET,
        absoluteWriteReadDataFrameTestPath + "/parquet")
      wdf.execute(executionContext)(Vector(convertedDataFrame))

      val rdf = ReadDataFrame(
        absoluteWriteReadDataFrameTestPath + "/parquet",
        FileFormat.PARQUET)
      val loadedDataFrame = rdf.execute(executionContext)(Vector()).head.asInstanceOf[DataFrame]

      assertDataFramesEqual(loadedDataFrame, dataFrame, checkRowOrder = false)
    }

    // Issue with TimestampType will be fixed in Spark 1.5.1
    // https://issues.apache.org/jira/browse/SPARK-10632
/*    "write and read JSON file" in {
      val convertedSchema = schema.copy()
      val timestampFieldIndex = convertedSchema.fieldIndex("timestamp")

      convertedSchema.fields.update(
        timestampFieldIndex,
        StructField(
          "timestamp",
          SparkConversions.columnTypeToSparkColumnType(ColumnType.string)))

      // JSON format does not completely preserve schema.
      // Timestamp columns are converted to string columns by WriteDataFrame operation.
      val convertedRows = rows.map {
        r => {
          if (r.get(timestampFieldIndex) != null) {
            val seq = r.toSeq
            Row.fromSeq(seq.updated(timestampFieldIndex, r.get(timestampFieldIndex).toString))
          } else {
            r
          }
        }
      }
      val convertedDataFrame = createDataFrame(convertedRows, convertedSchema)

      val wdf = WriteDataFrame(
        FileFormat.JSON,
        absoluteWriteReadDataFrameTestPath + "/json")
      wdf.execute(executionContext)(Vector(dataFrame))

      val rdf = ReadDataFrame(
        absoluteWriteReadDataFrameTestPath + "/json",
        FileFormat.JSON,
        categoricalColumns = Some(MultipleColumnSelection(
          Vector(NameColumnSelection(Set("categorical")))
        ))
      )
      val loadedDataFrame = rdf.execute(executionContext)(Vector()).head.asInstanceOf[DataFrame]

      assertDataFramesEqual(loadedDataFrame, convertedDataFrame, checkRowOrder = false)
    }*/
  }
}
