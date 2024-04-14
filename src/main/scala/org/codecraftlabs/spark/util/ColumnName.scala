package org.codecraftlabs.spark.util

object ColumnName extends Enumeration {
  type ColumnName = String
  val Id = "id"
  val CaseNumber = "caseNumber"
  val Date = "date"
  val Block = "block"
  val PrimaryType = "primaryType"
  val Description = "description"
  val LocationDescription = "locationDescription"
  val Timestamp = "timestamp"
  val Year = "year"
  val Month = "month"
  val Count = "count"
  val Iucr = "iucr"
  val Arrest = "arrest"
  val Domestic = "domestic"
  val District = "district"
  val Ward = "ward"
  val CommunityArea = "communityArea"
  val FbiCode = "fbiCode"
  val XCoordinate = "xCoordinate"
  val YCoordinate = "yCoordinate"
  val UpdatedOn = "updatedOn"
  val Latitude = "latitude"
  val Longitude = "longitude"
  val Location = "location"
}
