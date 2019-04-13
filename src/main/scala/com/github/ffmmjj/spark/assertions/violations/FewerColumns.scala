package com.github.ffmmjj.spark.assertions.violations

import org.apache.spark.sql.DataFrame

class FewerColumns(expected: DataFrame, actual: DataFrame) extends EqualityViolations {
  private val expectedDfColumns = expected.columns
  private val actualDfColumns = actual.columns
  private val missingColumnsInActualDf = expectedDfColumns.toSet.diff(actualDfColumns.toSet).toSeq

  override def notFound: Boolean = missingColumnsInActualDf.isEmpty

  override def toString: String =
    s"Dataframe ${actual.toString} doesn't have column(s) [${missingColumnsInActualDf.mkString(", ")}]"
}
