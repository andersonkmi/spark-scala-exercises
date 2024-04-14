package org.codecraftlabs.spark.util

import org.apache.spark.sql.types._
import org.codecraftlabs.spark.util.ColumnName.{Arrest, Block, CaseNumber, CommunityArea, Date, Description, District, Domestic, FbiCode, Id, Iucr, Latitude, Location, LocationDescription, Longitude, PrimaryType, UpdatedOn, Ward, XCoordinate, YCoordinate, Year}

object SchemaDefinition {
  def chicagoCrimeDatasetSchemaDefinition(): StructType = {
    StructType(Array(
      StructField(Id, LongType, nullable = false),
      StructField(CaseNumber, StringType, nullable = false),
      StructField(Date, StringType, nullable = false),
      StructField(Block, StringType, nullable = false),
      StructField(Iucr, StringType, nullable = false),
      StructField(PrimaryType, StringType, nullable = false),
      StructField(Description, StringType, nullable = false),
      StructField(LocationDescription, StringType, nullable = false),
      StructField(Arrest, BooleanType, nullable = false),
      StructField(Domestic, BooleanType, nullable = false),
      StructField(District, StringType, nullable = true),
      StructField(Ward, StringType, nullable = true),
      StructField(CommunityArea, StringType, nullable = true),
      StructField(FbiCode, StringType, nullable = true),
      StructField(XCoordinate, LongType, nullable = true),
      StructField(YCoordinate, LongType, nullable = true),
      StructField(Year, IntegerType, nullable = true),
      StructField(UpdatedOn, StringType, nullable = true),
      StructField(Latitude, StringType, nullable = true),
      StructField(Longitude, StringType, nullable = true),
      StructField(Location, StringType, nullable = true)
    ))
  }
}
