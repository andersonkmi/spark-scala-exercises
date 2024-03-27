package org.codecraftlabs.spark

import org.apache.spark.sql.SparkSession
import org.codecraftlabs.spark.util.SchemaDefinition.chicagoCrimeDatasetSchemaDefinition

object ChicagoCrimeDatasetExtractor {
  def main(args: Array[String]): Unit = {
    // Create the Spark session
    val spark = SparkSession.builder.appName("ChicagoCrimeDatasetExtractor").master("local[*]").getOrCreate()

    if (args.length < 2) {
      println("Usage: ChicagoCrimeDatasetExtractor <input_folder> <output_folder>")
      sys.exit(1)
    }

    // Defines the schema of the dataset
    val schemaDefinition = chicagoCrimeDatasetSchemaDefinition()
  }
}
