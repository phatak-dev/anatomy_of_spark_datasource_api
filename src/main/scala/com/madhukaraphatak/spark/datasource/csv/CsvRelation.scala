package com.madhukaraphatak.spark.datasource.csv

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Created by madhu on 20/6/15.
 */
class CsvRelation(location: String, separator:String,userSchema: StructType = null)(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  override def schema: StructType = {
    if (this.userSchema != null) userSchema
    else {
      //discover  the schema from the header
      val firstLine = sqlContext.sparkContext.textFile(location).first()
      val columnNames = firstLine.split(separator)
      val schemaFields = columnNames.map { fieldName =>
        StructField(fieldName, StringType, nullable = true)
      }
      StructType(schemaFields)

    }


  }

  override def buildScan(): RDD[Row] = ???
}
