package com.madhukaraphatak.spark.datasource.csv

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
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

  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with Serializable{
  @transient val logger = Logger.getLogger(classOf[CsvRelation])
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
    buildScan(schema.fieldNames)
  }
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {

    logger.info("pruned build scan for columns " + requiredColumns.toList)
    val rdd = sqlContext.sparkContext.textFile(location)
    val schemaFields = schema.fields
    val dataLines = rdd.filter(row => row != firstLine)
    val rowRDD = dataLines.map(row => {
      val columnValues = row.split(separator)
      //type cast to right type
      val typedValues = columnValues.zipWithIndex.map {
        case (value, index) => {
          val columnName = schemaFields(index).name
          val dataType = schemaFields(index).dataType
          val castedValue = castTo(value, dataType)
          if (requiredColumns.contains(columnName)) Some(castedValue)
          else None
        }
      }
      val selectedValues = typedValues.filter(_.isDefined).map(value => value.get)
      Row.fromSeq(selectedValues)
    })
    rowRDD
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    logger.info("pruned build scan for columns " + requiredColumns.toList + "with filters " + filters.toList)
    //purely optimization, so if you don't push filters no problem
    buildScan(requiredColumns)
  }
  private def castTo(value:String, dataType:DataType) = {

    dataType match {
      case  _:StringType => value
      case  _ : IntegerType => value.toInt
      case  _ : DoubleType => value.toDouble
    }
  }
}
