package com.github.ffmmjj.spark.assertions

import com.github.ffmmjj.spark.assertions.violations._
import org.apache.spark.sql.{DataFrame, SparkSession}


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
  private val spark: SparkSession = actual.sqlContext.sparkSession

  def shouldHaveSameContentsAs(expected: DataFrame, withAnyColumnOrdering: Boolean): Unit = {
    val fewerColumnsViolation = new FewerColumnsViolation(expected, actual)
    assert(fewerColumnsViolation.notFound, fewerColumnsViolation.toString)

    val extraColumnsViolation = new ExtraColumnsViolation(expected, actual)
    assert(extraColumnsViolation.notFound, extraColumnsViolation.toString)

    val differentColumnOrderingViolation = new DifferentColumnOrderingViolation(expected, actual)
    assert(withAnyColumnOrdering || differentColumnOrderingViolation.notFound, differentColumnOrderingViolation.toString)

    val differentNumberOfRowsViolation = new DifferentNumberOfRowsViolation(expected, actual)
    assert(differentNumberOfRowsViolation.notFound, differentNumberOfRowsViolation.toString)

    val differentColumnTypesViolation = new DifferentColumnTypesViolation(expected, actual)
    assert(differentColumnTypesViolation.notFound, differentColumnTypesViolation.toString)

    val rowsWithDifferentValuesViolation = new RowsWithDifferentValuesViolation(spark, expected, actual)
    assert(rowsWithDifferentValuesViolation.notFound, rowsWithDifferentValuesViolation.toString)
  }

  def shouldHaveSameContentsAs(expected: DataFrame): Unit = {
    shouldHaveSameContentsAs(expected, withAnyColumnOrdering = false)
  }

  def shouldHaveSameContentsAs(expected: ExpectedDataFrameWithIgnoredColumns): Unit = {
    shouldHaveSameContentsAs(expected.expected, withAnyColumnOrdering = true)
  }
}