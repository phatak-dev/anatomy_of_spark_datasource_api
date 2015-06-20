package com.madhukaraphatak.spark.datasource.csv

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.util.Try

/**
 * Base relation for the Csv Data
 */
class CsvRelation(location: String,
                  separator: String,
                  sampleRatio:Double,
                  userSchema: StructType = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable{

  private lazy val firstLine = {
    sqlContext.sparkContext.textFile(location).first()
  }

  override def schema: StructType = {
    if (this.userSchema != null) userSchema
    else {
      //discover  the schema from the header
      val columnNames = firstLine.split(separator)
      val sampleRDD = sqlContext.sparkContext.textFile(location).sample(false, sampleRatio)
      val dataLines = sampleRDD.filter(row => row != firstLine)

      //infer schema
      val dataTypes = dataLines.map { line =>
        val columnValues = line.split(separator)
        columnValues.zipWithIndex.map { case (value, index) => {
          val fieldType = inferField(value)
          StructField(columnNames(index), fieldType, nullable = true)
        }
        }
      }.first()
      StructType(dataTypes)

    }
  }

  private def inferField(value: String): DataType = {
    Try(value.toInt).map(value=>IntegerType).recoverWith {
      case e: Exception => Try(value.toDouble).map(value=>DoubleType)
    }.getOrElse(StringType)
  }

  override def buildScan(): RDD[Row] = {
    val rdd = sqlContext.sparkContext.textFile(location)
    val schemaFields = schema.fields
    val dataLines = rdd.filter(row => row != firstLine)
    val rowRDD = dataLines.map(row => {
      val columnValues = row.split(separator)
      //type cast to right type
      val typedValues = columnValues.zipWithIndex.map{
        case (value,index) => {
          val dataType = schemaFields(index).dataType
          castTo(value,dataType)
        }
      }
      Row.fromSeq(typedValues)
    })
    rowRDD
  }

  private def castTo(value:String, dataType:DataType) = {

    dataType match {
      case  _:StringType => value
      case  _ : IntegerType => value.toInt
      case  _ : DoubleType => value.toDouble
    }
  }
}
