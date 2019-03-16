package com.github.ffmmjj.spark.assertions

import com.github.ffmmjj.spark.assertions.DataFrameAssertions._
import com.github.ffmmjj.spark.helpers.SparkSessionTestWrapper
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.AccessShowString

import scala.util.Try


class DataFrameAssertionsTest extends FlatSpec with SparkSessionTestWrapper with Matchers {
  import spark.implicits._

  behavior of "shouldHaveSameContentsAs"
  it should "raise an exception detailing the missing fields if the expected dataframe has columns that don't exist in the actual dataframe" in {
    val actual = Seq("value1").toDF("field1")
    val expected = Seq(("value1", "value2", "value3")).toDF("field1", "field2", "field3")

    val assertionResult = Try(actual shouldHaveSameContentsAs expected)

    val expectedErrorMessage = s"${actual.toString()} doesn't have column(s) [field2, field3]"
    assertionResult.failed.get.getMessage should include (expectedErrorMessage)
  }

  it should "raise an exception detailing the extra fields if the actual dataframe has columns that dont't exist in the expected dataframe" in {
    val actual = Seq(("value1", "value2", "value3")).toDF("field1", "field2", "field3")
    val expected = Seq("value1").toDF("field1")

    val assertionResult = Try(actual shouldHaveSameContentsAs expected)

    val expectedErrorMessage = s"${actual.toString()} contains extra columns [field2, field3]"
    assertionResult.failed.get.getMessage should include (expectedErrorMessage)
  }

  it should "raise an exception if the columns in the actual and expected dataframe have the same names but different types" in {
    val actual = Seq(("value1", 2.0.toLong)).toDF("field1", "field2")
    val expected = Seq((1, 2.0)).toDF("field1", "field2")

    val assertionResult = Try(actual shouldHaveSameContentsAs expected)

    val failureMessage = assertionResult.failed.get.getMessage
    failureMessage should include ("Columns have different types.")
    failureMessage should include ("Expected: (field1, IntegerType$), (field2, DoubleType$)")
    failureMessage should include ("Actual: (field1, StringType$), (field2, LongType$)")
  }

  it should "raise an exception if the columns in the actual and expected dataframes follow a different order" in {
    val actual = Seq(("value1", "value2")).toDF("field1", "field2")
    val expected = Seq(("value2", "value1")).toDF("field2", "field1")

    val assertionResult = Try(actual shouldHaveSameContentsAs expected)

    val expectedErrorMessage = s"${actual.toString} has the same columns as ${expected.toString}, but in a different order - do you really care about column order in this test?"
    assertionResult.failed.get.getMessage should include (expectedErrorMessage)
  }

  it should "not raise an exception if the columns in the actual and expected dataframes follow a different order but the withAnyColumnOrdering argument is set to true" in {
    val actual = Seq(("value1", "value2")).toDF("field1", "field2")
    val expected = Seq(("value2", "value1")).toDF("field2", "field1")

    val assertionResult = Try(actual shouldHaveSameContentsAs(expected, withAnyColumnOrdering=true))

    assertionResult.isSuccess should be (true)
  }

  it should "not raise an exception if the columns in the actual and expected dataframes follow a different order but the withAnyColumnOrdering qualifier is used" in {
    val actual = Seq(("value1", "value2")).toDF("field1", "field2")
    val expected = Seq(("value2", "value1")).toDF("field2", "field1")

    val assertionResult = Try(actual shouldHaveSameContentsAs (expected withAnyColumnOrdering))

    assertionResult.isSuccess should be (true)
  }

  it should "raise an exception if the columns are the same but the values differ in some of the dataframe lines" in {
    val actual = Seq(
      ("value1", "value2", "value3"),
      ("value4", "value5", "value6")
    ).toDF("field1", "field2", "field3")
    val expected = Seq(
      ("value1", "value7", "value8"),
      ("value9", "value5", "value6")
    ).toDF("field1", "field2", "field3")
    val expectedMismatchesFromActualDfSummary = Seq(
      (0, null, "value2", "value3"),
      (1, "value4", null, null)
    ).toDF("line", "field1", "field2", "field3")
    val expectedMismatchesFromExpectedDfSummary = Seq(
      (0, null, "value7", "value8"),
      (1, "value9", null, null)
    ).toDF("line", "field1", "field2", "field3")

    val failureMessage = Try(actual shouldHaveSameContentsAs expected).failed.get.getMessage

    failureMessage should include ("Different values found in some lines.")
    failureMessage should include ("Mismatched values in actual DataFrame:")
    failureMessage should include (AccessShowString.showString(expectedMismatchesFromActualDfSummary, 2))
    failureMessage should include ("Mismatched values in expected DataFrame:")
    failureMessage should include (AccessShowString.showString(expectedMismatchesFromExpectedDfSummary, 2))
  }
}
