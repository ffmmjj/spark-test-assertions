package com.github.ffmmjj.spark.assertions

import org.apache.spark.sql.DataFrame


object DataFrameAssertions {
  implicit def dataFrameToDataFrameWithCustomAssertions(actual: DataFrame): DataFrameWithCustomAssertions = DataFrameWithCustomAssertions(actual)
}

case class DataFrameWithCustomAssertions(actual: DataFrame) {
  def shouldHaveSameContentsAs(expected: DataFrame): Unit = {
    val expectedDfColumns = expected.columns.toSet
    val actualDfColumns = actual.columns.toSet
    val missingColumnsInActualDf = expectedDfColumns.diff(actualDfColumns).toSeq
    val extraColumnInActualDf = actualDfColumns.diff(expectedDfColumns).toSeq

    assert(missingColumnsInActualDf.isEmpty, s"Dataframe ${actual.toString} doesn't have column(s) [${String.join(", ", missingColumnsInActualDf:_*)}]")
    assert(extraColumnInActualDf.isEmpty, s"Dataframe ${actual.toString} contains extra columns [${String.join(", ", extraColumnInActualDf:_*)}]")
  }
}