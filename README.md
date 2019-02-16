# spark-test-assertions
A single assertion to compare DataFrames.

## Basic usage
`import com.github.ffmmjj.spark.assertions.DataFrameAssertions._`

and then, in your test,

`actualDataframe shouldHaveSameContentsAs expectedDataframe`

## Examples
```scala
it should "raise an exception detailing the missing fields if the expected dataframe has columns that don't exist in the actual dataframe" in {
  val actual = Seq("value1").toDF("field1")
  val expected = Seq(("value1", "value2", "value3")).toDF("field1", "field2", "field3")

  actual shouldHaveSameContentsAs expected
}
```

Will output an exception with message 
```
assertion failed: Dataframe [field1: string] doesn't have column(s) [field2, field3]
```


```scala
it should "raise an exception detailing the extra fields if the actual dataframe has columns that dont't exist in the expected dataframe" in {
  val actual = Seq(("value1", "value2", "value3")).toDF("field1", "field2", "field3")
  val expected = Seq("value1").toDF("field1")

  actual shouldHaveSameContentsAs expected
}
```

Will output an exception with message 
```
assertion failed: Dataframe [field1: string, field2: string ... 1 more field] contains extra columns [field2, field3]
```

```scala
it should "raise an exception if the columns in the actual and expected dataframes follow a different order" in {
  val actual = Seq(("value1", "value2")).toDF("field1", "field2")
  val expected = Seq(("value2", "value1")).toDF("field2", "field1")
    
  actual shouldHaveSameContentsAs expected
}
```

Will output an exception with message 
```
assertion failed: DataFrame [field1: string, field2: string] has the same columns as [field2: string, field1: string], but in a different order - do you really care about column order in this test?
```

```scala
it should "raise an exception if the columns are the same but the values differ in some of the dataframe lines" in {
  val actual = Seq(
    ("value1", "value2", "value3"),
    ("value4", "value5", "value6")
  ).toDF("field1", "field2", "field3")
  val expected = Seq(
    ("value1", "value7", "value8"),
    ("value9", "value5", "value6")
  ).toDF("field1", "field2", "field3")

  actual shouldHaveSameContentsAs expected
}
```

Will output an exception with message 
```
assertion failed: Different values found.
Line 0: {field2: (expected value7, found value2), field3: (expected value8, found value3)}
Line 1: {field1: (expected value9, found value4)}
```
