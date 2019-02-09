package com.github.ffmmjj.spark.assertions

import org.apache.spark.sql.DataFrame


object DataFrameAssertions {
  implicit def dataFrameToDataFrameWithCustomAssertions(actual: DataFrame): DataFrameWithCustomAssertions = DataFrameWithCustomAssertions(actual)
}

case class DataFrameWithCustomAssertions(actual: DataFrame) {
  def shouldHaveSameContentsAs(expected: DataFrame): Unit = {
    val expectedDfColumns = expected.columns
    val actualDfColumns = actual.columns
    val missingColumnsInActualDf = expectedDfColumns.toSet.diff(actualDfColumns.toSet).toSeq
    val extraColumnInActualDf = actualDfColumns.toSet.diff(expectedDfColumns.toSet).toSeq

    assert(missingColumnsInActualDf.isEmpty, buildMissingColumnsMessage(missingColumnsInActualDf))
    assert(extraColumnInActualDf.isEmpty, buildExtraColumnsMessage(extraColumnInActualDf))
    assert(actualDfColumns sameElements expectedDfColumns, buildColumnsInDifferentOrderMessage(expected))
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