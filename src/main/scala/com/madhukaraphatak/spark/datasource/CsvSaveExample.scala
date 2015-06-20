package com.madhukaraphatak.spark.datasource

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
  * Save as CSV
  */
object CsvSaveExample {
   def main(args: Array[String]) {

     val sc = new SparkContext(args(0), "Csv loading example")
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     val df = sqlContext.load("org.apache.spark.sql.json", Map("path" -> args(1)))
     df.save("com.madhukaraphatak.spark.datasource.csv",SaveMode.Overwrite, Map("path" -> args(2)))
   }
 }
