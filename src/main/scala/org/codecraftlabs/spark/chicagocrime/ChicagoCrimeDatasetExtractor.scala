package org.codecraftlabs.spark.chicagocrime

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, desc}

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

  def countCrimeGroupedByColumn(df: DataFrame, columnName: String, isSortedAscending: Boolean = true): DataFrame = {
    val countDF = df.groupBy(col(columnName)).count()
    if (isSortedAscending) countDF.orderBy(asc("count")) else countDF.orderBy(desc("count"))
  }
}
