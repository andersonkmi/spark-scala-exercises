package org.codecraftlabs.spark.util

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object SchemaDefinition {
  def chicagoCrimeDatasetSchemaDefinition(): StructType = {
    StructType(Array(
      StructField("Id", LongType, nullable = false),
      StructField("CaseNumber", StringType, nullable = false),
      StructField("Date", StringType, nullable = false),
      StructField("Block", StringType, nullable = false),
    ))
  }
}
