package org.codecraftlabs.spark.chicagocrime

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class ChicagoCrimeDataExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  @transient var sparkSession: Option[SparkSession] = None
  override def beforeAll(): Unit = {
    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.master", "local")
    sparkSession = Some(SparkSession.builder().config(sparkConfig).getOrCreate())
  }

  private def createDataFrame(): DataFrame = {
    val sampleData = Seq(
      Row(11646166L, "JC213529", "09/01/2018 12:01:00 AM", "082XX S INGLESIDE AVE", "0810", "THEFT", "OVER $500", "RESIDENCE", false, true),
      Row(11645836L, "JC212333", "05/01/2016 12:25:00 AM", "055XX S ROCKWELL ST", "1153", "DECEPTIVE PRACTICE", "FINANCIAL IDENTITY THEFT OVER $ 300", "", false, true)
    )
    val schema = chicagoCrimeDatasetSchemaDefinition()
    sparkSession.get.createDataFrame(sparkSession.get.sparkContext.parallelize(sampleData), schema)
  }

  "When setting up the raw dataframe" must "return a valid dataframe" in {
    val df = createDataFrame()
    df.count() mustEqual 2
  }

  private def chicagoCrimeDatasetSchemaDefinition(): StructType = {
    StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("caseNumber", StringType, nullable = false),
      StructField("date", StringType, nullable = false),
      StructField("block", StringType, nullable = false),
      StructField("iucr", StringType, nullable = false),
      StructField("primaryType", StringType, nullable = false),
      StructField("description", StringType, nullable = false),
      StructField("locationDescription", StringType, nullable = false),
      StructField("arrest", BooleanType, nullable = false),
      StructField("domestic", BooleanType, nullable = false)
    ))
  }
}
