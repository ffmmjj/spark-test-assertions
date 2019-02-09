package com.github.ffmmjj.spark.helpers

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Spark test assertions library")
      .getOrCreate()
  }

}
