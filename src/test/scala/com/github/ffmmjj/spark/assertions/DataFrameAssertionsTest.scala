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
    val expected = Seq(("value4", "value5", "value6")).toDF("field1", "field2", "field3")

    val assertionResult = Try(actual shouldHaveSameContentsAs expected)

    val expectedErrorMessage = s"${actual.toString()} doesn't have column(s) [field2, field3]"
    assertionResult.failed.get.getMessage should include (expectedErrorMessage)
  }
}
