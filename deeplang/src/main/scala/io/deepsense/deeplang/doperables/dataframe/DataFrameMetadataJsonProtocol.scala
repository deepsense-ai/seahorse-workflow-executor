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

package io.deepsense.deeplang.doperables.dataframe

import spray.httpx.SprayJsonSupport
import spray.json._

import io.deepsense.commons.json.EnumerationSerializer
import io.deepsense.commons.types.ColumnType
import io.deepsense.deeplang.doperables.dataframe.types.categorical.{CategoricalColumnMetadata, CategoriesMapping}
import io.deepsense.deeplang.doperables.dataframe.types.vector.VectorColumnMetadata
import io.deepsense.deeplang.doperables.dataframe.types.{ColumnMetadata, EmptyColumnMetadata}

// TODO DS-1759 improve error handling
trait CategoriesMappingJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  implicit object CategoriesMappingFormat extends JsonFormat[CategoriesMapping] {
    override def write(cm: CategoriesMapping): JsValue = cm.valueToId.keys.toVector.toJson

    override def read(value: JsValue): CategoriesMapping =
      CategoriesMapping(value.convertTo[Seq[String]])
  }
}

trait ColumnKnowledgeJsonProtocol
  extends DefaultJsonProtocol
  with SprayJsonSupport
  with CategoriesMappingJsonProtocol
  with NullOptions {

  implicit val columnTypeFormat = EnumerationSerializer.jsonEnumFormat(ColumnType)

  private object ColumnMetadataJsonProtocol {
    implicit object CategoricalColumnMetadataFormat extends JsonFormat[CategoricalColumnMetadata] {
      private val CategoriesKey = "categories"

      override def write(obj: CategoricalColumnMetadata): JsValue = JsObject(
        CategoriesKey -> obj.categories.toJson)

      override def read(json: JsValue): CategoricalColumnMetadata = {
        json.asJsObject.fields.get(CategoriesKey) match {
          case Some(categories) =>
            CategoricalColumnMetadata(categories.convertTo[CategoriesMapping])
          case None => throw deserializationError(s"Expected field '$CategoriesKey' not found")
        }
      }
    }

    implicit val vectorColumnMetadataFormat = jsonFormat1(VectorColumnMetadata)

    implicit object ColumnMetadataFormat extends JsonFormat[ColumnMetadata] {
      override def write(obj: ColumnMetadata): JsValue = obj match {
        case EmptyColumnMetadata => JsObject()
        case categorical: CategoricalColumnMetadata => categorical.toJson
        case vector: VectorColumnMetadata => vector.toJson
      }

      override def read(json: JsValue): ColumnMetadata = EmptyColumnMetadata
    }
  }

  implicit object ColumnKnowledgeFormat extends JsonFormat[ColumnKnowledge] {
    import ColumnMetadataJsonProtocol._

    val defaultFormat = jsonFormat4(ColumnKnowledge.apply)

    override def write(ck: ColumnKnowledge): JsValue = ck.toJson(defaultFormat)

    override def read(json: JsValue): ColumnKnowledge = {
      val withoutMetadata = json.convertTo[ColumnKnowledge](defaultFormat)

      val categoricalType = ColumnType.categorical.toString
      val vectorType = ColumnType.vector.toString

      val metadata = json.asJsObject.getFields("columnType", "metadata") match {
        case Seq(JsNull, _) => None
        case Seq(_, JsNull) => None
        case Seq(typeJs, metadataJs) =>
          Some(typeJs.convertTo[String] match {
            case `categoricalType` => metadataJs.convertTo[CategoricalColumnMetadata]
            case `vectorType` => metadataJs.convertTo[VectorColumnMetadata]
            case _ => EmptyColumnMetadata
          })
      }
      withoutMetadata.copy(metadata = metadata)
    }
  }
}

trait DataFrameMetadataJsonProtocol
    extends DefaultJsonProtocol
    with SprayJsonSupport
    with ColumnKnowledgeJsonProtocol {
  implicit val dataFrameMetadataFormat = jsonFormat3(DataFrameMetadata.apply)
}

object DataFrameMetadataJsonProtocol extends DataFrameMetadataJsonProtocol
