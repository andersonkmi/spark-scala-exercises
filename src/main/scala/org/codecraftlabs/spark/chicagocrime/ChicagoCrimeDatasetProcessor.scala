package org.codecraftlabs.spark.chicagocrime

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.codecraftlabs.spark.util.SchemaDefinition.chicagoCrimeDatasetSchemaDefinition

object ChicagoCrimeDatasetProcessor {
  @transient private lazy val logger: Logger = Logger.getLogger("ChicagoCrimeDatasetExtractor")
  private val Csv: String = "csv"
  private val Header: String = "header"
  private val True:String = "true"
  private val Overwrite: String = "overwrite"
  private val CrimeCountPerPrimaryTypeFolder = "crime_count_per_primary_type"
  private val CrimeCountPerPrimaryTypeYearMonthFolder = "crime_count_per_primary_type_year_month"
  private val CrimeCountPerYearPrimaryTypeFolder = "crime_count_per_year_primary_type"

  private val chicagoCrimeDatasetExtractor = new ChicagoCrimeDatasetExtractor

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("ChicagoCrimeDatasetExtractor").master("local[*]").getOrCreate()

    if (args.length < 2) {
      println("Usage: ChicagoCrimeDatasetExtractor <input_folder> <output_folder>")
      sys.exit(1)
    }

    val inputFolder = args(0)
    val outputFolder = args(1)
    logger.info(s"Input folder provided: '$inputFolder'")
    logger.info(s"Output folder provided: '$outputFolder'")

    // Extracts the main columns
    val schemaDefinition = chicagoCrimeDatasetSchemaDefinition()
    val df = spark.read.format(Csv).option(Header, True).schema(schemaDefinition).load(inputFolder)
    val extractedDF = chicagoCrimeDatasetExtractor.extractInitialDataset(df)

    // Filter rows that contains a valid date
    val dataFrameWithDate = chicagoCrimeDatasetExtractor.filterRowsWithDate(extractedDF)

    // Insert a timestamp column
    val dfWithTimestamp = chicagoCrimeDatasetExtractor.addTimestampColumn(dataFrameWithDate, "date")

    // Extracts the crime primary types
    val primaryTypeDF = chicagoCrimeDatasetExtractor.extractDistinctValuesFromSingleColumn("primaryType",
      dfWithTimestamp,
      sorted = true)
    primaryTypeDF.write.format(Csv).option(Header, True).mode(Overwrite).save(s"$outputFolder/primaryType")

    // Number of crime per primary type
    val crimeCountPerPrimaryType = chicagoCrimeDatasetExtractor.countCrimeGroupedByColumn(extractedDF, "primaryType")
    saveDataFrameToCsv(crimeCountPerPrimaryType, s"$outputFolder/$CrimeCountPerPrimaryTypeFolder")

    // Group crime count per year, month, primaryType
    val dfWithYearMonth = chicagoCrimeDatasetExtractor.addYearAndMonthColumns(dfWithTimestamp)
    val crimeCountGroupedByYearMonthPrimaryType = chicagoCrimeDatasetExtractor.countCrimeGroupedByTypeYearMonth(dfWithYearMonth)
    saveDataFrameToCsv(crimeCountGroupedByYearMonthPrimaryType, s"$outputFolder/$CrimeCountPerPrimaryTypeYearMonthFolder")

    // Group crime count per year and primary type - single partition
    val crimeCountGroupedByYearPrimaryType = chicagoCrimeDatasetExtractor.countCrimeGroupedByPrimaryTypeYear(dfWithYearMonth)
    saveDataFrameToCsv(crimeCountGroupedByYearPrimaryType, s"$outputFolder/$CrimeCountPerYearPrimaryTypeFolder/all_years")

    val yearsDF = chicagoCrimeDatasetExtractor.extractDistinctValuesFromSingleColumn("year", crimeCountGroupedByYearMonthPrimaryType, sorted = true)
    val yearsList = yearsDF.collect().toList.map(item => item.getString(0))
    yearsList.foreach(year => filterByYearAndSaveCsv(crimeCountGroupedByYearPrimaryType, year, outputFolder))
  }

  private def filterByYearAndSaveCsv(df: DataFrame, year: String, outputFolder: String): Unit = {
    logger.info(s"Filtering crimes per year - current value '$year'")
    val dfPerYear = df.where(col("year") === year).drop(col("year"))
    saveDataFrameToCsv(dfPerYear, s"$outputFolder/$CrimeCountPerYearPrimaryTypeFolder/$year")
  }

  private def saveDataFrameToCsv(df: DataFrame, destination: String): Unit = {
    df.write.format(Csv).option(Header, True).mode(Overwrite).save(destination)
  }
}
