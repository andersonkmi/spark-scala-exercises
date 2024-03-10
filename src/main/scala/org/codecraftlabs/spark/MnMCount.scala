package org.codecraftlabs.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc}

object MnMCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("MnMCount").getOrCreate()

    if (args.length < 1) {
      println("Usage: MnMCount <mnm_file_dataset>")
      sys.exit(1)
    }

    // Get the M&M data set file name
    val mnmFile = args(0)

    // Read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // Aggregate counts of all colors and groupBy() State and Color
    // orderBy() in descending order
    val countMnMDF = mnmDF.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    // Show the resulting aggregations for all the states and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // Find the aggregate counts for California by filtering
    val caCountMnMDF = mnmDF.select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    caCountMnMDF.show(10)
    spark.stop()
  }
}
