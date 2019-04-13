package com.github.ffmmjj.spark.assertions.violations

import org.apache.spark.sql.DataFrame

class DifferentColumnOrderingViolation(expected: DataFrame, actual: DataFrame) extends EqualityViolation {
  override def notFound: Boolean = actual.columns sameElements expected.columns

  override def toString: String =
    s"DataFrame ${actual.toString} has the same columns as ${expected.toString}, but in a different order - " +
      s"do you really care about column order in this test?"
}
