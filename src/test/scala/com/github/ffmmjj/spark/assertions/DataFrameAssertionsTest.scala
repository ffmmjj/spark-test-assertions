package com.github.ffmmjj.spark.assertions

import com.github.ffmmjj.spark.helpers.SparkSessionTestWrapper
import org.scalatest.{FlatSpec, Matchers}
import com.github.ffmmjj.spark.assertions.DataFrameAssertions._

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

  it should "raise an exception if the columns in the actual and expected dataframes follow a different order" in {
    val actual = Seq(("value1", "value2")).toDF("field1", "field2")
    val expected = Seq(("value2", "value1")).toDF("field2", "field1")

    val assertionResult = Try(actual shouldHaveSameContentsAs expected)

    val expectedErrorMessage = s"${actual.toString} has the same columns as ${expected.toString}, but in a different order - do you really care about column order in this test?"
    assertionResult.failed.get.getMessage should include (expectedErrorMessage)
  }

  it should "not raise an exception if the columns in the actual and expected dataframes follow a different order but the ignoringColumnsOrder is set to false" in {
    val actual = Seq(("value1", "value2")).toDF("field1", "field2")
    val expected = Seq(("value2", "value1")).toDF("field2", "field1")

    val assertionResult = Try(actual shouldHaveSameContentsAs(expected, ignoringColumnsOrder=true))

    assertionResult.isSuccess should be (true)
  }
}
