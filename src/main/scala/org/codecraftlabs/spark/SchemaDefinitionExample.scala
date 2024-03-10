package org.codecraftlabs.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object SchemaDefinitionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SchemaDefinitionExample").getOrCreate()

    if (args.length <= 0) {
      println("Usage SchemaDefinitionExample <file.json>")
      System.exit(1)
    }

    val jsonFile = args(0)
    val schema = StructType(Array(
      StructField("Id", IntegerType, nullable = false),
      StructField("First", StringType, nullable = false),
      StructField("Last", StringType, nullable = false),
      StructField("Url", StringType, nullable = false),
      StructField("Published", StringType, nullable = false),
      StructField("Hits", IntegerType, nullable = false),
      StructField("Campaigns", ArrayType(StringType), nullable = false)
    ))

    // /Create a DataFrame by reading from the JSON file with a predefined schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    blogsDF.show(false)
    println(blogsDF.printSchema)
    println(blogsDF.schema)
  }
}
