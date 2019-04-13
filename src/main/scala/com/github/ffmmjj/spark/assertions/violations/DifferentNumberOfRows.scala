package com.github.ffmmjj.spark.assertions.violations

import org.apache.spark.sql.DataFrame

class DifferentNumberOfRows(expected: DataFrame, actual: DataFrame) extends EqualityViolations {
  private val expectedDfRowCount = expected.count()
  private val actualDfRowCount = actual.count()

  override def notFound: Boolean = expectedDfRowCount == actualDfRowCount

  override def toString: String =
    s"""
       |"The number of rows in the actual dataframe is different than in the expected dataframe."
       |Expected: $expectedDfRowCount
       |Actual: $actualDfRowCount
      """.stripMargin
}
