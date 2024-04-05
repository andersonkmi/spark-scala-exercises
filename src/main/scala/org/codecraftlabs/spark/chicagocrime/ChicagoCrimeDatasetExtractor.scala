package org.codecraftlabs.spark.chicagocrime

import org.apache.spark.sql.functions.{asc, desc}
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

  def extractDistinctValuesFromSingleColumn(columnName: String,
                                            df: DataFrame,
                                            sorted: Boolean = false,
                                            isAscendingOrder: Boolean = false): DataFrame = {
    if (sorted) {
      if (isAscendingOrder) df.select(columnName).distinct().sort(asc(columnName)) else df.select(columnName).distinct().sort(desc(columnName))
    } else {
      df.select(columnName)
    }
  }
}
