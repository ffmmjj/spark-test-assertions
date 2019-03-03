package org.apache.spark.sql

// Implementation obtained from the link below:
// https://stackoverflow.com/a/51229630
object AccessShowString {
  def showString[T](df: Dataset[T],
                    _numRows: Int, truncate: Int = 20): String = {
    df.showString(_numRows, truncate)
  }
}