package org.codecraftlabs.spark.chicagocrime

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, desc, unix_timestamp}

class ChicagoCrimeDatasetExtractor {
  def extractInitialDataset(df: DataFrame): DataFrame = {
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
                                            isAscendingOrder: Boolean = true): DataFrame = {
    val dfWithDistinctValues = df.select(columnName).distinct()
    if (sorted) {
      if (isAscendingOrder) dfWithDistinctValues.sort(asc(columnName)) else dfWithDistinctValues.sort(desc(columnName))
    } else {
      dfWithDistinctValues
    }
  }

  def countCrimeGroupedByColumn(df: DataFrame,
                                columnName: String,
                                isSortedAscending: Boolean = true): DataFrame = {
    val countDF = df.groupBy(col(columnName)).count()
    if (isSortedAscending) countDF.orderBy(asc("count")) else countDF.orderBy(desc("count"))
  }

  def addTimestampColumn(df: DataFrame, columnName: String): DataFrame = {
    df.withColumn("timestamp", unix_timestamp(col(columnName), "MM/dd/yyyy HH:mm:ss a"))
  }
}
