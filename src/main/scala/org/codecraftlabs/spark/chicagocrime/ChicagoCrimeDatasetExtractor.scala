package org.codecraftlabs.spark.chicagocrime

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.codecraftlabs.spark.util.ColumnName.{Block, CaseNumber, Count, Date, Description, Id, LocationDescription, Month, PrimaryType, Timestamp, Year}

class ChicagoCrimeDatasetExtractor {
  private val adjustPrimaryTypeValue = udf{(originalValue: String) =>
    if (originalValue.equals("CRIM SEXUAL ASSAULT")) {
      "CRIMINAL SEXUAL ASSAULT"
    } else if (originalValue.equals("NON - CRIMINAL")) {
      "NON-CRIMINAL"
    }
    else {
      originalValue
    }
  }

  def extractInitialDataset(df: DataFrame): DataFrame = {
    df.select(
      Id,
      CaseNumber,
      Date,
      Block,
      PrimaryType,
      Description,
      LocationDescription)
      .withColumn(
        PrimaryType,
        adjustPrimaryTypeValue(df(PrimaryType)))
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
    if (isSortedAscending) countDF.orderBy(asc(Count)) else countDF.orderBy(desc(Count))
  }

  def filterRowsWithDate(df: DataFrame): DataFrame = {
    df.where(col(Date).isNotNull)
  }

  def addTimestampColumn(df: DataFrame,
                         columnName: String): DataFrame = {
    df.withColumn(Timestamp, unix_timestamp(col(columnName), "MM/dd/yyyy HH:mm:ss a").cast(TimestampType))
  }

  def addYearAndMonthColumns(df: DataFrame): DataFrame = {
    df.withColumn(Year, date_format(col(Timestamp), "yyyy"))
      .withColumn(Month, date_format(col(Timestamp), "MM"))
  }

  def countCrimeGroupedByTypeYearMonth(df: DataFrame): DataFrame = {
    val initialGrouping = df.groupBy(col(Year), col(Month), col(PrimaryType))
      .count()
      .orderBy(asc(Year), asc(Month), asc(PrimaryType))
    dropItemsWithoutYear(initialGrouping)
  }

  def countCrimeGroupedByPrimaryTypeYear(df: DataFrame): DataFrame = {
    val initialDF = df.groupBy(col(Year), col(PrimaryType))
      .count()
      .orderBy(asc(Year), asc(PrimaryType))
    dropItemsWithoutYear(initialDF)
  }

  private def dropItemsWithoutYear(df: DataFrame): DataFrame = {
    df.where(col(Year).isNotNull)
  }
}
