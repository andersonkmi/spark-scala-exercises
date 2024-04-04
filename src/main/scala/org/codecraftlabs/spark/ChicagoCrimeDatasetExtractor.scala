package org.codecraftlabs.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.codecraftlabs.spark.util.SchemaDefinition.chicagoCrimeDatasetSchemaDefinition

object ChicagoCrimeDatasetExtractor {
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

    // Defines the schema of the dataset
    logger.info("Load schema definition")
    val schemaDefinition = chicagoCrimeDatasetSchemaDefinition()

    // Reads the file(s)
    val df = spark.read.format("csv").option("header", "true").schema(schemaDefinition).load(inputFolder)
    df.printSchema()
    val rowCount = df.count()
    logger.info(s"Total number of rows '$rowCount'")

    // Extracts some columns: id, case number, date, block, primary type, description, location description, year
    val extractedDF = df.select("id",
      "caseNumber",
      "date",
      "block",
      "primaryType",
      "description",
      "locationDescription")
    val primaryTypeDF = extractedDF.select("primaryType").distinct()

    // Writes the current dataframe back
    primaryTypeDF.write.format("csv").mode("overwrite").save(s"$outputFolder/primaryType")
  }
}
