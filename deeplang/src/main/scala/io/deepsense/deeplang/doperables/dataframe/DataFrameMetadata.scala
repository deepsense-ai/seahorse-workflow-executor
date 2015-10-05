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

import org.apache.spark.sql.types.{StructField, StructType}
import spray.json._

import io.deepsense.commons.types.ColumnType
import io.deepsense.commons.types.ColumnType.ColumnType
import io.deepsense.deeplang._
import io.deepsense.deeplang.doperables.dataframe.DataFrameMetadataJsonProtocol._
import io.deepsense.deeplang.doperables.dataframe.types.categorical.{CategoriesMapping, CategoricalColumnMetadata}
import io.deepsense.deeplang.doperables.dataframe.types.vector.VectorColumnMetadata
import io.deepsense.deeplang.doperables.dataframe.types.{NonEmptyColumnMetadata, EmptyColumnMetadata, ColumnMetadata, SparkConversions}
import io.deepsense.deeplang.doperations.exceptions.{ColumnDoesNotExistException, ColumnsDoNotExistException}
import io.deepsense.deeplang.inference.exceptions.NameNotUniqueException
import io.deepsense.deeplang.inference.{InferenceWarning, InferenceWarnings, MultipleColumnsMayNotExistWarning, SingleColumnMayNotExistWarning}
import io.deepsense.deeplang.parameters._

/**
 * Metadata of DataFrame.
 * Can represent partial or missing information.
 * @param isExact Indicates if all information inside metadata is exact. It is true if and only if
 *                  1. [[isColumnCountExact]] is true
 *                  2. all [[columns]] fields are set to Some
 * @param isColumnCountExact Indicates if size of [[columns]] is exact. If not, there is possibility
 *                           that actual DataFrame will have more columns than [[columns]].
 * @param columns It contains information about columns.
 */
case class DataFrameMetadata(
    isExact: Boolean,
    isColumnCountExact: Boolean,
  columns: Map[String, ColumnKnowledge])
  extends DOperable.AbstractMetadata {

  /**
   * @return Spark schema basing on information that it holds.
   * @throws IllegalStateException when called on DataFrameMetadata with isExact set to false
   */
  def toSchema: StructType = {
    if (!isExact) {
      throw new IllegalStateException(
        "Cannot call toSchema on DataFrameMetadata with isExact field set to false")
    }
    val sortedColumns = columns.values.toList.sortBy(_.index.get)
    val structFields = sortedColumns.map(_.toStructField)
    StructType(structFields)
  }

  /**
   * Appends a column to knowledge. If column count is exact, index of new column is calculated
   * precisely. Otherwise index is set to unknown (None). In any case, index provided in column
   * knowledge is ignored.
   * Throws [[NameNotUniqueException]] if name is not unique.
   */
  def appendColumn(columnKnowledge: ColumnKnowledge): DataFrameMetadata = {
    val newIndex = if (isColumnCountExact) Some(columns.size) else None
    val columnWithIndex = columnKnowledge.copy(index = newIndex)
    val name = columnKnowledge.name
    if (columns.contains(name)) {
      throw NameNotUniqueException(name)
    }
    copy(columns = columns + (name -> columnWithIndex))
  }

  /**
   * Selects column from knowledge based on provided SingleColumnSelection.
   * If isExact field is true and provided selector points to
   * column that is not in current metadata, ColumnDoesNotExistException will be thrown.
   * If isExact field is false and provided selector points to
   * column that is not in current metadata, InferenceWarnings will contain appropriate warning
   * and no exception will be thrown.
   */
  @throws[ColumnDoesNotExistException]
  def select(
    columnSelection: SingleColumnSelection): (Option[ColumnKnowledge], InferenceWarnings) = {
    val knowledgeOption = getKnowledgeOption(columnSelection)
    if (knowledgeOption.isEmpty) {
      if (isExact) {
        throw ColumnDoesNotExistException(columnSelection, this)
      }
      (None, InferenceWarnings(SingleColumnMayNotExistWarning(columnSelection, this)))
    } else {
      (knowledgeOption, InferenceWarnings.empty)
    }
  }

  /**
   * Selects columns from knowledge based on provided MultipleColumnSelection.
   * If isExact field is true and provided selector points to
   * columns that are not in current metadata, ColumnsDoNotExistException will be thrown.
   * If isExact field is false and provided selector points to
   * columns that are not in current metadata, InferenceWarnings will contain appropriate warning
   * and no exception will be thrown.
   *
   * Returned Seq[ColumnMetadata] will be subset of current schema,
   * the order of columns will be preserved as in orderedColumns method.
   */
  @throws[ColumnsDoNotExistException]
  def select(
    multipleColumnSelection: MultipleColumnSelection):
  (Seq[ColumnKnowledge], InferenceWarnings) = {
    val warnings = assertColumnSelectionsValid(multipleColumnSelection)
    val selectedColumns = for {
      column <- orderedColumns
      selection <- multipleColumnSelection.selections
      if isFieldSelected(column.name, column.index, column.columnType, selection)
    } yield column

    val selected = selectedColumns.toSeq.distinct
    val inferenceWarnings = InferenceWarnings(warnings.toVector)
    if (multipleColumnSelection.excluding) {
      (orderedColumns.filterNot(selected.contains(_)), inferenceWarnings)
    } else {
      (selected, inferenceWarnings)
    }
  }

  /**
   * @return Columns ordered by index.
   *         Columns without index will be listed at the end of the sequence.
   */
  def orderedColumns: Seq[ColumnKnowledge] = {
    val values = columns.values
    val columnsSortedByIndex = values.filter(_.index.isDefined).toList.sortBy(_.index.get)
    val columnsWithoutIndex = values.filter(_.index.isEmpty).toList
    columnsSortedByIndex ++ columnsWithoutIndex
  }

  /**
   * @return Some[ColumnKnowledge] if columnSelection selects column, None otherwise
   */
  private def getKnowledgeOption(
    columnSelection: SingleColumnSelection): Option[ColumnKnowledge] = {
    columnSelection match {
      case nameSelection: NameSingleColumnSelection =>
        columns.get(nameSelection.value)
      case indexSelection: IndexSingleColumnSelection =>
        columns.values.find(_.index.exists(index => index == indexSelection.value))
    }
  }

  /**
   * Tells if column is selected by given selection.
   * Out-of-range indexes and non-existing column names are ignored.
   * @param columnName Name of field.
   * @param columnIndex Index of field in schema.
   * @param columnType Type of field's column.
   * @param selection Selection of columns.
   * @return True iff column meets selection's criteria.
   */
  private def isFieldSelected(
      columnName: String,
      columnIndex: Option[Int],
      columnType: Option[ColumnType],
      selection: ColumnSelection): Boolean = selection match {
    case IndexColumnSelection(indexes) => columnIndex.exists(indexes.contains(_))
    case NameColumnSelection(names) => names.contains(columnName)
    case TypeColumnSelection(types) => columnType.exists(types.contains(_))
    case IndexRangeColumnSelection(Some(lowerBound), Some(upperBound)) =>
      columnIndex.exists(index => (index >= lowerBound && index <= upperBound))
    case IndexRangeColumnSelection(_, _) => false
  }

  private def assertColumnSelectionsValid(
    multipleColumnSelection: MultipleColumnSelection): Seq[InferenceWarning] = {
    val selections = multipleColumnSelection.selections
    val invalidSelections = selections.filterNot(isSelectionValid)
    if (invalidSelections.nonEmpty && isExact) {
      throw ColumnsDoNotExistException(invalidSelections, toSchema)
    }
    invalidSelections.map(MultipleColumnsMayNotExistWarning(_, this))
  }

  /**
   * Checks if given selection is valid with regard to dataframe schema.
   * Returns false if some specified names or indexes are incorrect.
   */
  private def isSelectionValid(selection: ColumnSelection): Boolean = selection match {
    case IndexColumnSelection(indexes) =>
      val metadataIndexes = indexesSet
      val indexesIntersection = metadataIndexes & indexes.toSet
      indexesIntersection.size == indexes.size
    case NameColumnSelection(names) =>
      val metadataNames = columns.keys.toSet
      val namesIntersection = names.toSet & metadataNames
      namesIntersection.size == names.size
    case TypeColumnSelection(_) => true
    case IndexRangeColumnSelection(Some(lowerBound), Some(upperBound)) =>
      val metadataIndexes = indexesSet
      (lowerBound to upperBound).toSet.subsetOf(metadataIndexes)
    case IndexRangeColumnSelection(_, _) => true
  }

  private def indexesSet: Set[Int] = {
    columns.values.map(_.index).flatten.toSet
  }

  override protected def _serializeToJson = this.toJson
}

object DataFrameMetadata {

  def empty: DataFrameMetadata = DataFrameMetadata(
    isExact = false, isColumnCountExact = false, columns = Map.empty)

  def fromSchema(schema: StructType): DataFrameMetadata = {
    DataFrameMetadata(
      isExact = true,
      isColumnCountExact = true,
      columns = schema.zipWithIndex.map({ case (structField, index) =>
        val rawResult = ColumnKnowledge.fromStructField(structField, index)
        rawResult.name -> rawResult
      }).toMap
    )
  }

  def deserializeFromJson(jsValue: JsValue): DataFrameMetadata = {
    DOperable.AbstractMetadata.unwrap(jsValue).convertTo[DataFrameMetadata]
  }

  def buildColumnsMap(columns: Seq[ColumnKnowledge]): Map[String, ColumnKnowledge] = {
    columns.map(column => column.name -> column).toMap
  }
}

/**
 * Represents knowledge about a column in DataFrame.
 * @param name Name of column - always known.
 * @param index Index of this column in DataFrame. None denotes unknown index.
 * @param columnType Type of this column. None denotes unknown type.
 * @param metadata Type specific metadata of this column. None denotes unknown metadata.
 *                 Note: it is possible that in future we will have to support partially known
 *                 metadata. In that case, we will need to construct a hierarchy of
 *                 ColumnMetadataKnowledge that will mirror ColumnMetadata hierarchy.
 */
case class ColumnKnowledge private[dataframe] (
    name: String,
    index: Option[Int],
    columnType: Option[ColumnType],
    metadata: Option[ColumnMetadata]) {

  metadata.foreach {
    case m: NonEmptyColumnMetadata =>
      require(m.columnType == columnType.get, "Metadata and columnType do not conform")
    case _ => ()
  }

  def prettyPrint: String = SchemaPrintingUtils.columnKnowledgeToString(this)

  /** Assumes that this knowledge contains full information. */
  private[dataframe] def toStructField: StructField = {
    require(index.isDefined && columnType.isDefined && metadata.isDefined,
      "Cannot create StructField from partial knowledge about column.")
    StructField(
      name = name,
      dataType = SparkConversions.columnTypeToSparkColumnType(columnType.get),
      metadata = metadata.get.toSparkMetadata()
    )
  }
}

object ColumnKnowledge {

  private[dataframe] def fromStructField(structField: StructField, index: Int): ColumnKnowledge = {
    ColumnKnowledge(
      name = structField.name,
      index = Some(index),
      columnType = Some(SparkConversions.sparkColumnTypeToColumnType(structField.dataType)),
      metadata = Some(ColumnMetadata.fromStructField(structField))
    )
  }

  def apply(
      name: String,
      index: Option[Int],
      columnType: Option[ColumnType]): ColumnKnowledge = {
    columnType.foreach { case ct =>
      require(ct != ColumnType.vector, "Use ColumnKnowledge.vector constructor")
      require(ct != ColumnType.categorical, "Use ColumnKnowledge.categorical constructor")
    }
    ColumnKnowledge(name, index, columnType, Some(EmptyColumnMetadata))
  }

  def categorical(
      name: String,
      index: Option[Int],
      metadata: Option[CategoricalColumnMetadata]): ColumnKnowledge = {
    ColumnKnowledge(name, index, Some(ColumnType.categorical), metadata)
  }

  def categorical(
      name: String,
      index: Option[Int],
      mapping: CategoriesMapping): ColumnKnowledge = {
    ColumnKnowledge.categorical(name, index, Some(CategoricalColumnMetadata(mapping)))
  }

  def vector(
      name: String,
      index: Option[Int],
      metadata: Option[VectorColumnMetadata]): ColumnKnowledge = {
    ColumnKnowledge(name, index, Some(ColumnType.vector), metadata)
  }

  def vector(
      name: String,
      index: Option[Int],
      length: Long): ColumnKnowledge = {
    ColumnKnowledge.vector(name, index, Some(VectorColumnMetadata(length)))
  }
}
