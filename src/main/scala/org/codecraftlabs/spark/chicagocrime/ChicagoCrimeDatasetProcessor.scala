package org.codecraftlabs.spark.chicagocrime

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.codecraftlabs.spark.util.SchemaDefinition.chicagoCrimeDatasetSchemaDefinition

object ChicagoCrimeDatasetProcessor {
  @transient private lazy val logger: Logger = Logger.getLogger("ChicagoCrimeDatasetExtractor")
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

    // Extracts the main columns
    val schemaDefinition = chicagoCrimeDatasetSchemaDefinition()
    val df = spark.read.format("csv").option("header", "true").schema(schemaDefinition).load(inputFolder)

    val extractedDF = chicagoCrimeDatasetExtractor.extractInitialDataset(df)
    val primaryTypeDF = chicagoCrimeDatasetExtractor.extractDistinctValuesFromSingleColumn("primaryType",
      extractedDF,
      sorted = true,
      isAscendingOrder = true)
    primaryTypeDF.write.format("csv").option("header", "true").mode("overwrite").save(s"$outputFolder/primaryType")
  }
}
