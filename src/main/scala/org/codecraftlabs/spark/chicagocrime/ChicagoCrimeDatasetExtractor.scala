package org.codecraftlabs.spark.chicagocrime

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, desc}

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
    if (sorted) {
      if (isAscendingOrder) df.select(columnName).distinct().sort(asc(columnName)) else df.select(columnName).distinct().sort(desc(columnName))
    } else {
      df.select(columnName).distinct()
    }
  }
}
