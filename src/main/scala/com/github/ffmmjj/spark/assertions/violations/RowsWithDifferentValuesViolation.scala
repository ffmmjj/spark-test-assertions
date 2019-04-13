package com.github.ffmmjj.spark.assertions.violations

import com.github.ffmmjj.spark.assertions.{ColumnValueMismatch, ValueMismatchesInLine}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{AccessShowString, DataFrame, Row, SparkSession}

class RowsWithDifferentValuesViolation(spark: SparkSession, expected: DataFrame, actual: DataFrame) extends EqualityViolation {
  private lazy val linesWithUnmatchedValues = getLinesWithUnmatchedValues

  override def notFound: Boolean = linesWithUnmatchedValues.isEmpty

  override def toString: String = {
    val columnsWithUnmatchedValues = linesWithUnmatchedValues.flatMap(_.columnsMismatches.keys).sorted
    val mismatchesInExpectedDf = lineMismatchesToValueRows(linesWithUnmatchedValues, columnsWithUnmatchedValues,
      (columnMismatch: ColumnValueMismatch) => columnMismatch.expectedValue)
    val mismatchesInActualDf = lineMismatchesToValueRows(linesWithUnmatchedValues, columnsWithUnmatchedValues,
      (columnMismatch: ColumnValueMismatch) => columnMismatch.actualValue)
    val mismatchesDfSchema = StructType(
      Seq(StructField("line", IntegerType, nullable = false)) ++
        columnsWithUnmatchedValues.map(colName => StructField(colName, StringType, nullable = true))
    )
    val expectedMismatchesDf = spark.createDataFrame(
      spark.sparkContext.parallelize(mismatchesInExpectedDf),
      mismatchesDfSchema
    )
    val actualMismatchesDf = spark.createDataFrame(
      spark.sparkContext.parallelize(mismatchesInActualDf),
      mismatchesDfSchema
    )

    "Different values found in some lines.\n" +
      "Mismatched values in actual DataFrame:\n" +
      AccessShowString.showString(actualMismatchesDf, mismatchesInActualDf.size) +
      "Mismatched values in expected DataFrame:\n" +
      AccessShowString.showString(expectedMismatchesDf, mismatchesInExpectedDf.size)
  }

  private def getLinesWithUnmatchedValues = {
    actual.collect().zip(expected.collect())
      .zipWithIndex
      .map { case (rows, lineNo) => ValueMismatchesInLine(lineNo, unmatchingValues(rows._1, rows._2)) }
      .filter (_.columnsMismatches.nonEmpty)
  }

  private def lineMismatchesToValueRows(linesWithUnmatchedValues: Array[ValueMismatchesInLine],
                                        columnsWithUnmatchedValues: Array[String],
                                        getMismatchedValue: ColumnValueMismatch => String) = {
    linesWithUnmatchedValues
      .map(lineMismatch =>
        Seq(lineMismatch.lineNumber) ++
          columnsWithUnmatchedValues.map(
            col => lineMismatch.columnsMismatches.get(col).map(getMismatchedValue).orNull
          ).toSeq
      )
      .map(Row(_: _*))
      .toSeq
  }

  private def unmatchingValues(actualRow: Row, expectedRow: Row) = {
    actual.schema
      .filter(schemaField => {
        schemaField.dataType match {
          case StringType => actualRow.getAs[String](schemaField.name) != expectedRow.getAs[String](schemaField.name)
          case DoubleType => Math.abs(actualRow.getAs[Double](schemaField.name) - expectedRow.getAs[Double](schemaField.name)) > 0.001
          case FloatType => Math.abs(actualRow.getAs[Float](schemaField.name) - expectedRow.getAs[Float](schemaField.name)) > 0.001
          case IntegerType => actualRow.getAs[Int](schemaField.name) != expectedRow.getAs[Int](schemaField.name)
          case LongType => actualRow.getAs[Long](schemaField.name) != expectedRow.getAs[Long](schemaField.name)
        }
      })
      .map(schemaField => {
        val actualValueAsString = Option(actualRow.getAs[Any](schemaField.name)).map(_.toString).getOrElse("null")
        val expectedValueAsString = Option(expectedRow.getAs[Any](schemaField.name)).map(_.toString).getOrElse("null")

        schemaField.name -> ColumnValueMismatch(schemaField.name, actualValueAsString, expectedValueAsString)
      })
      .toMap
  }
}
