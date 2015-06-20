package com.madhukaraphatak.spark.datasource.csv

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Default source for Csv
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext,parameters,null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    //check parameters
    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))
    val separator = parameters.get("separator").getOrElse(",")
    val sampleRatio= parameters.get("sampleRatio").map(_.toDouble).getOrElse(1.0)
    new CsvRelation(parameters.get("path").get,separator = separator,sampleRatio,schema)(sqlContext)
  }
}
