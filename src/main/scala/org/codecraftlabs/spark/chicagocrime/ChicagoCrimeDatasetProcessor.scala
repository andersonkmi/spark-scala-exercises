package org.codecraftlabs.spark.chicagocrime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ChicagoCrimeDatasetProcessor {
  @transient private lazy val logger: Logger = Logger.getLogger("ChicagoCrimeDatasetExtractor")
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    // Create the Spark session
    val spark = SparkSession.builder.appName("ChicagoCrimeDatasetExtractor").master("local[*]").getOrCreate()

    if (args.length < 2) {
      println("Usage: ChicagoCrimeDatasetExtractor <input_folder> <output_folder>")
      sys.exit(1)
    }

    // Saves the arguments passed to the job
    // Not validating if the folders exist - just assume the user knows what he/she is doing
    val inputFolder = args(0)
    val outputFolder = args(1)
    logger.info(s"Input folder provided: '$inputFolder'")

    // Executing first step
    val chicagoCrimeDatasetExtractor = new ChicagoCrimeDatasetExtractor
    val extractedDF = chicagoCrimeDatasetExtractor.extractInitialDataset(spark, inputFolder)
    val primaryTypeDF = extractedDF.select("primaryType").distinct()

    // Writes the current dataframe back
    primaryTypeDF.write.format("csv").mode("overwrite").save(s"$outputFolder/primaryType")
  }
}
