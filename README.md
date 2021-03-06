# spark-test-assertions
A single assertion to compare Spark DataFrames and Datasets (future work).

## Basic usage
`libraryDependencies += "com.github.ffmmjj" % "spark-test-assertions_2.11" % "<version>"`

and then

`import com.github.ffmmjj.spark.assertions.DataFrameAssertions._`

and then, in your test,

`actualDataframe shouldHaveSameContentsAs expectedDataframe`

## Goals
1. Minimal set of dependencies (only Spark itself as a provided dependency);
2. Readable error messages (clearly pointing differences instead of just dumping the expected and actual DataFrames to the output);
3. Flexible and readable assertion constraints (allowing, for example, to ignore column order in the compared DataFrames);

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
it should "raise an exception if the number of rows in the actual dataframe is different than in the expected dataframe" in {
  val actual = Seq(
    ("value1", 7.68910)
  ).toDF("field1", "field2")
  val expected = Seq(
    ("value1", 7.68910),
    ("value2", 15.161718)
  ).toDF("field1", "field2")

  actual shouldHaveSameContentsAs expected
```

Will output an exception with message
```
The number of rows in the actual dataframe is different than in the expected dataframe.
Expected: 2
Actual: 1
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

_It's possible, however, to add the `withAnyColumnOrdering` qualifier to prevent column ordering from being checked like `actual shouldHaveSameContentsAs (expected withAnyColumnOrdering)` (parenthesis required) or as an explicit argument like `actual.shouldHaveSameContentsAs(expected, withAnyColumnOrdering=true)`_

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
assertion failed: Different values found in some lines.
Mismatched values in actual DataFrame:
+----+------+------+------+
|line|field1|field2|field3|
+----+------+------+------+
|   0|  null|value2|value3|
|   1|value4|  null|  null|
+----+------+------+------+
Mismatched values in expected DataFrame:
+----+------+------+------+
|line|field1|field2|field3|
+----+------+------+------+
|   0|  null|value7|value8|
|   1|value9|  null|  null|
+----+------+------+------+
```
