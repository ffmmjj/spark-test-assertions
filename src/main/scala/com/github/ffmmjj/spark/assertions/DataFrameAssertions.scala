package com.github.ffmmjj.spark.assertions

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}


object DataFrameAssertions {
  implicit def dataFrameToDataFrameWithCustomAssertions(actual: DataFrame): DataFrameWithCustomAssertions = DataFrameWithCustomAssertions(actual)
  implicit def dataFrameToExpectedDataFrameWithIgnoredColumns(expected: DataFrame): ExpectedDataFrameWithIgnoredColumns = ExpectedDataFrameWithIgnoredColumns(expected)
}

case class ExpectedDataFrameWithIgnoredColumns(expected: DataFrame) {
  def withAnyColumnOrdering: ExpectedDataFrameWithIgnoredColumns = this
}

case class ColumnValueMismatch(columnName: String, actualValue: String, expectedValue: String)

case class ValueMismatchesInLine(lineNumber: Int, columnsMismatches: Map[String, ColumnValueMismatch])

case class DataFrameWithCustomAssertions(actual: DataFrame) {
  def shouldHaveSameContentsAs(expected: DataFrame, withAnyColumnOrdering: Boolean): Unit = {
    val expectedDfColumns = expected.columns
    val actualDfColumns = actual.columns
    val missingColumnsInActualDf = expectedDfColumns.toSet.diff(actualDfColumns.toSet).toSeq
    val extraColumnInActualDf = actualDfColumns.toSet.diff(expectedDfColumns.toSet).toSeq

    assert(missingColumnsInActualDf.isEmpty, buildMissingColumnsMessage(missingColumnsInActualDf))
    assert(extraColumnInActualDf.isEmpty, buildExtraColumnsMessage(extraColumnInActualDf))
    assert(withAnyColumnOrdering || (actualDfColumns sameElements expectedDfColumns), buildColumnsInDifferentOrderMessage(expected))

    val columnsWithDifferentTypes = getColumnsWithDifferentTypes(expected)
    assert(columnsWithDifferentTypes.isEmpty, buildColumnsWithDifferentTypesMessage(columnsWithDifferentTypes))

    val linesWithUnmatchedValues = getLinesWithUnmatchedValues(expected)
    assert(linesWithUnmatchedValues.isEmpty, buildUnmatchedValuesMessage(linesWithUnmatchedValues))
  }

  def shouldHaveSameContentsAs(expected: DataFrame): Unit = {
    shouldHaveSameContentsAs(expected, withAnyColumnOrdering = false)
  }

  def shouldHaveSameContentsAs(expected: ExpectedDataFrameWithIgnoredColumns): Unit = {
    shouldHaveSameContentsAs(expected.expected, withAnyColumnOrdering = true)
  }

  private def getColumnsWithDifferentTypes(expected: DataFrame) = {
    actual.schema.map(schemaField => (schemaField.name, schemaField.dataType)).sortBy(_._1)
      .zip(expected.schema.map(schemaField => (schemaField.name, schemaField.dataType)).sortBy(_._1))
      .filter { case (actualItem, expectedItem) => actualItem._2 != expectedItem._2 }
  }

  private def getLinesWithUnmatchedValues(expected: DataFrame) = {
    actual.collect().zip(expected.collect())
      .zipWithIndex
      .map { case (rows, lineNo) => ValueMismatchesInLine(lineNo, unmatchingValues(rows._1, rows._2)) }
      .filter (_.columnsMismatches.nonEmpty)
  }

  private def buildColumnsWithDifferentTypesMessage(columnsWithDifferentTypes: Seq[((String, DataType), (String, DataType))]) = {
    val unmatchedColumnsFromActualDf = columnsWithDifferentTypes.map(_._1)
    val unmatchedColumnsFromExpectedDf = columnsWithDifferentTypes.map(_._2)

    val expectedTypesMessage = unmatchedColumnsFromExpectedDf.map { case (columnName, columnType) => s"($columnName, ${columnType.getClass.getSimpleName})"}
    val actualTypesMessage = unmatchedColumnsFromActualDf.map { case (columnName, columnType) => s"($columnName, ${columnType.getClass.getSimpleName})"}

    "Columns have different types.\n" +
      s"Expected: ${String.join(", ", expectedTypesMessage:_*)}\n"+
      s"Actual: ${String.join(", ", actualTypesMessage:_*)}"
  }

  private def buildUnmatchedValuesMessage(linesWithUnmatchedValues: Array[ValueMismatchesInLine]) = {
    val unmatchedValuesDescriptions = linesWithUnmatchedValues
      .map (lineMismatch =>
        s"Line ${lineMismatch.lineNumber}: {${String.join(", ", lineMismatch.columnsMismatches.map(item => s"${item._1}: (expected ${item._2.expectedValue}, found ${item._2.actualValue})").toSeq:_*)}}"
      )

    "Different values found.\n" + String.join("\n", unmatchedValuesDescriptions:_*)
  }

  private def unmatchingValues(actualRow: Row, expectedRow: Row) = {
    actual.schema
      .filter(schemaField => {
        schemaField.dataType match {
          case StringType => actualRow.getAs[String](schemaField.name) != expectedRow.getAs[String](schemaField.name)
          case DoubleType => Math.abs(actualRow.getAs[Double](schemaField.name) - expectedRow.getAs[Double](schemaField.name)) > 0.001
          case FloatType => Math.abs(actualRow.getAs[Float](schemaField.name) - expectedRow.getAs[Float](schemaField.name)) > 0.001
          case IntegerType => actualRow.getAs[Int](schemaField.name) != expectedRow.getAs[Int](schemaField.name)
          case LongType => actualRow.getAs[Long](schemaField.name) != expectedRow.getAs[Long](schemaField.name)
        }
      })
      .map(schemaField => {
        schemaField.name -> ColumnValueMismatch(schemaField.name, actualRow.getAs[String](schemaField.name), expectedRow.getAs[String](schemaField.name))
      })
      .toMap
  }

  private def buildMissingColumnsMessage(missingColumnsInActualDf: Seq[String]) = {
    s"Dataframe ${actual.toString} doesn't have column(s) [${String.join(", ", missingColumnsInActualDf: _*)}]"
  }

  private def buildExtraColumnsMessage(extraColumnInActualDf: Seq[String]) = {
    s"Dataframe ${actual.toString} contains extra columns [${String.join(", ", extraColumnInActualDf: _*)}]"
  }

  private def buildColumnsInDifferentOrderMessage(expected: DataFrame) = {
    s"DataFrame ${actual.toString} has the same columns as ${expected.toString}, but in a different order - " +
      s"do you really care about column order in this test?"
  }
}