package com.madhukaraphatak.spark.datasource.csv

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Created by madhu on 20/6/15.
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext,parameters,null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    //check parameters
    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))
    val separator = parameters.get("separator").getOrElse(",")
    new CsvRelation(parameters.get("path").get,separator = separator,schema)(sqlContext)
  }
}
