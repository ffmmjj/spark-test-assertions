package com.github.ffmmjj.spark.assertions

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.ffmmjj.spark.assertions.violations._


object DataFrameAssertions {
  implicit def dataFrameToDataFrameWithCustomAssertions(actual: DataFrame): DataFrameWithCustomAssertions = DataFrameWithCustomAssertions(actual)
  implicit def dataFrameToExpectedDataFrameWithIgnoredColumns(expected: DataFrame): ExpectedDataFrameWithIgnoredColumns = ExpectedDataFrameWithIgnoredColumns(expected)
}

case class DataFrameWithCustomAssertions(actual: DataFrame) {
  private val spark: SparkSession = actual.sqlContext.sparkSession

  def shouldHaveSameContentsAs(expected: DataFrame, withAnyColumnOrdering: Boolean): Unit = {
    Seq(
      new FewerColumnsViolation(expected, actual),
      new ExtraColumnsViolation(expected, actual),
      new DifferentColumnOrderingViolation(expected, actual, withAnyColumnOrdering),
      new DifferentNumberOfRowsViolation(expected, actual),
      new DifferentColumnTypesViolation(expected, actual),
      new RowsWithDifferentValuesViolation(spark, expected, actual)
    ).foreach(v => assert(v.notFound, v.toString))
  }

  def shouldHaveSameContentsAs(expected: DataFrame): Unit = {
    shouldHaveSameContentsAs(expected, withAnyColumnOrdering = false)
  }

  def shouldHaveSameContentsAs(expected: ExpectedDataFrameWithIgnoredColumns): Unit = {
    shouldHaveSameContentsAs(expected.expected, withAnyColumnOrdering = true)
  }
}

case class ExpectedDataFrameWithIgnoredColumns(expected: DataFrame) {
  def withAnyColumnOrdering: ExpectedDataFrameWithIgnoredColumns = this
}
