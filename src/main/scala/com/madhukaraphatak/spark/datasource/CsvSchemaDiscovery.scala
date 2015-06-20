package com.madhukaraphatak.spark.datasource

import org.apache.spark.SparkContext

/**
 *  Example code to discover schema
 */
object CsvSchemaDiscovery {
  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Csv loading example")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.load("com.madhukaraphatak.spark.datasource.csv", Map("path" -> args(1), "header" -> "true"))
    df.printSchema()

  }
}
