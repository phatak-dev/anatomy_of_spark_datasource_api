package com.madhukaraphatak.spark.datasource.csv

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{CreatableRelationProvider, BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Default source for Csv
 */
class DefaultSource extends RelationProvider with
SchemaRelationProvider with CreatableRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    //check parameters
    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))
    val separator = parameters.get("separator").getOrElse(",")
    val sampleRatio = parameters.get("sampleRatio").map(_.toDouble).getOrElse(1.0)
    new CsvRelation(parameters.get("path").get, separator = separator, sampleRatio, schema)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))
    val path = parameters.get("path").get

    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${
            this.getClass.getCanonicalName
          }")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }

    if (doSave) {
      // Only save data when the save mode is not ignore.
      saveAsCsvFile(data, path)
    }
    createRelation(sqlContext, parameters, data.schema)
  }

  def saveAsCsvFile(dataFrame: DataFrame, path: String) = {
    val header = dataFrame.columns.mkString(",")
    val strRDD = dataFrame.rdd.mapPartitionsWithIndex { case (index, iter) => {

      new Iterator[String] {
        var firstRow: Boolean = true

        override def hasNext = iter.hasNext || firstRow

        override def next: String = {
          if (!iter.isEmpty) {
            val rowValue = iter.next().toSeq.map(value => value.toString)
            val row = rowValue.mkString(",")
            if (firstRow) {
              firstRow = false
              header + "\n" + row
            } else {
              row
            }
          } else {
            firstRow = false
            header
          }
        }
      }
    }
    }
    strRDD.saveAsTextFile(path)
  }
}
