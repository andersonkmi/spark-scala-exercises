package org.codecraftlabs.spark.chicagocrime

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.codecraftlabs.spark.util.SchemaDefinition.chicagoCrimeDatasetSchemaDefinition

object ChicagoCrimeDatasetProcessor {
  @transient private lazy val logger: Logger = Logger.getLogger("ChicagoCrimeDatasetExtractor")
  private val Csv: String = "csv"
  private val Header: String = "header"
  private val True:String = "true"
  private val Overwrite: String = "overwrite"
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

    // Insert a timestamp column
    val dfWithTimestamp = chicagoCrimeDatasetExtractor.addTimestampColumn(extractedDF, "date")

    // Extracts the crime primary types
    val primaryTypeDF = chicagoCrimeDatasetExtractor.extractDistinctValuesFromSingleColumn("primaryType",
      dfWithTimestamp,
      sorted = true)
    primaryTypeDF.write.format(Csv).option(Header, True).mode(Overwrite).save(s"$outputFolder/primaryType")

    // Number of crime per primary type
    val crimeCountPerPrimaryType = chicagoCrimeDatasetExtractor.countCrimeGroupedByColumn(extractedDF, "primaryType")
    crimeCountPerPrimaryType.write.format(Csv).option(Header, True).mode(Overwrite).save(s"$outputFolder/crime_count_per_primary_type")

    // Group crime count per year, month, primaryType
    val dfWithYearMonth = chicagoCrimeDatasetExtractor.addYearAndMonthColumns(dfWithTimestamp)
    val crimeCountGroupedByYearMonthPrimaryType = chicagoCrimeDatasetExtractor.countCrimeGroupedByTypeYearMonth(dfWithYearMonth)
    crimeCountGroupedByYearMonthPrimaryType.write.format(Csv).option(Header, True).mode(Overwrite).save(s"$outputFolder/crime_count_per_primary_type_year_month")
  }
}
