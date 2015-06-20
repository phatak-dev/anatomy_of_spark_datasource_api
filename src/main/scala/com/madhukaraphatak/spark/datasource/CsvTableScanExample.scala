package com.madhukaraphatak.spark.datasource

import org.apache.spark.SparkContext

/**
  * Created by madhu on 20/6/15.
  */
object CsvTableScanExample {
   def main(args: Array[String]) {
     val sc = new SparkContext(args(0), "Csv loading example")
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     val df = sqlContext.load("com.madhukaraphatak.spark.datasource.csv", Map("path" -> args(1), "header" -> "true"))
     println(df.count())
     println(df.collectAsList())
   }
 }
