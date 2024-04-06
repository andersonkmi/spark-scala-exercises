package org.codecraftlabs.spark.util

import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}

object SchemaDefinition {
  def chicagoCrimeDatasetSchemaDefinition(): StructType = {
    StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("caseNumber", StringType, nullable = false),
      StructField("date", StringType, nullable = false),
      StructField("block", StringType, nullable = false),
      StructField("iucr", StringType, nullable = false),
      StructField("primaryType", StringType, nullable = false),
      StructField("description", StringType, nullable = false),
      StructField("locationDescription", StringType, nullable = false),
      StructField("arrest", BooleanType, nullable = false),
      StructField("domestic", BooleanType, nullable = false),
      StructField("district", StringType, nullable = true),
      StructField("ward", StringType, nullable = true),
      StructField("communityArea", StringType, nullable = true),
      StructField("fbiCode", StringType, nullable = true),
      StructField("xCoordinate", LongType, nullable = true),
      StructField("yCoordinate", LongType, nullable = true),
      StructField("year", IntegerType, nullable = true),
      StructField("updatedOn", StringType, nullable = true),
      StructField("latitude", StringType, nullable = true),
      StructField("longitude", StringType, nullable = true),
      StructField("location", StringType, nullable = true)
    ))
  }
}
