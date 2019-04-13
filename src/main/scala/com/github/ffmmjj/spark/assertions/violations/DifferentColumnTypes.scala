package com.github.ffmmjj.spark.assertions.violations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

class DifferentColumnTypes(expected: DataFrame, actual: DataFrame) extends EqualityViolations {
  private val columnsWithDifferentTypes = getColumnsWithDifferentTypes

  override def notFound: Boolean = columnsWithDifferentTypes.isEmpty

  override def toString: String = {
    val unmatchedColumnsFromActualDf = columnsWithDifferentTypes.map(_._1)
    val unmatchedColumnsFromExpectedDf = columnsWithDifferentTypes.map(_._2)

    val expectedTypesMessage = unmatchedColumnsFromExpectedDf.map { case (columnName, columnType) => s"($columnName, ${columnType.getClass.getSimpleName})" }
    val actualTypesMessage = unmatchedColumnsFromActualDf.map { case (columnName, columnType) => s"($columnName, ${columnType.getClass.getSimpleName})" }

    "Columns have different types.\n" +
      s"Expected: ${expectedTypesMessage.mkString(", ")}\n" +
      s"Actual: ${actualTypesMessage.mkString(", ")}"
  }

  private def getColumnsWithDifferentTypes: Seq[((String, DataType), (String, DataType))] = {
    actual.schema.map(schemaField => (schemaField.name, schemaField.dataType)).sortBy(_._1)
      .zip(expected.schema.map(schemaField => (schemaField.name, schemaField.dataType)).sortBy(_._1))
      .filter { case (actualItem, expectedItem) => actualItem._2 != expectedItem._2 }
  }
}
