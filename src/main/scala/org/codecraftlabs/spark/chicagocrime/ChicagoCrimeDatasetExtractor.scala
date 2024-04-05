package org.codecraftlabs.spark.chicagocrime

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.codecraftlabs.spark.util.SchemaDefinition.chicagoCrimeDatasetSchemaDefinition

class ChicagoCrimeDatasetExtractor {
  def extractInitialDataset(spark: SparkSession, inputFolder: String): DataFrame = {
    val schemaDefinition = chicagoCrimeDatasetSchemaDefinition()
    val df = spark.read.format("csv").option("header", "true").schema(schemaDefinition).load(inputFolder)
    df.select("id",
      "caseNumber",
      "date",
      "block",
      "primaryType",
      "description",
      "locationDescription")
  }

  def extractDistinctValuesFromSingleColumn(columnName: String, df: DataFrame): DataFrame = {
    df.select(columnName).distinct()
  }
}
