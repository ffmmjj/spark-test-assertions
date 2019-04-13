package com.github.ffmmjj.spark.assertions.violations

import org.apache.spark.sql.DataFrame

class ExtraColumnsViolation(expected: DataFrame, actual: DataFrame) extends EqualityViolation {
  private val expectedDfColumns = expected.columns
  private val actualDfColumns = actual.columns

  private val extraColumnInActualDf = actualDfColumns.toSet.diff(expectedDfColumns.toSet).toSeq

  override def notFound: Boolean = extraColumnInActualDf.isEmpty

  override def toString: String =
    s"Dataframe ${actual.toString} contains extra columns [${extraColumnInActualDf.mkString(", ")}]"
}
