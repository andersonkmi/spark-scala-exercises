package org.codecraftlabs.spark.chicagocrime

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class ChicagoCrimeDataExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  @transient var sparkSession: Option[SparkSession] = None
  private val chicagoCrimeDatasetExtractor: ChicagoCrimeDatasetExtractor = new ChicagoCrimeDatasetExtractor
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
      Row(11646166L,
        "JC213529",
        "09/01/2018 12:01:00 AM",
        "082XX S INGLESIDE AVE",
        "0810",
        "THEFT",
        "OVER $500",
        "RESIDENCE",
        false,
        true),
      Row(11645836L,
        "JC212333",
        "05/01/2016 12:25:00 AM",
        "055XX S ROCKWELL ST",
        "1153",
        "DECEPTIVE PRACTICE",
        "FINANCIAL IDENTITY THEFT OVER $ 300",
        "",
        false,
        true),
      Row(
        11034701L,
        "JA366925",
        "01/01/2001 11:00:00 AM",
        "016XX E 86TH PL",
        "1153",
        "DECEPTIVE PRACTICE",
        "FINANCIAL IDENTITY THEFT OVER $ 300",
        "RESIDENCE",
        false,
        false
      )
    )
    val schema = chicagoCrimeDatasetSchemaDefinition()
    sparkSession.get.createDataFrame(sparkSession.get.sparkContext.parallelize(sampleData), schema)
  }

  "When setting up the raw dataframe" must "return a valid dataframe" in {
    val df = createDataFrame()
    df.count() mustEqual 3
  }

  "When extracting some columns" must "return a DF with a subset of the fields" in {
    val expectedFields = List("id", "caseNumber", "date", "block", "primaryType", "description", "locationDescription")
    val df = createDataFrame()
    val extractedDF = chicagoCrimeDatasetExtractor.extractInitialDataset(df)
    val fieldNames = extractedDF.schema.map(item => item.name)
    fieldNames mustEqual expectedFields
  }

  "When calling extractDistinctValuesFromSingleColumn for primaryType column" must "return distinct values for the primaryType" in {
    val expectedValues: List[String] = List("THEFT", "DECEPTIVE PRACTICE")
    val initialDF = chicagoCrimeDatasetExtractor.extractInitialDataset(createDataFrame())
    val distinctPrimaryTypeDataframe = chicagoCrimeDatasetExtractor.extractDistinctValuesFromSingleColumn("primaryType", initialDF)
    val primaryTypeValues = distinctPrimaryTypeDataframe.collect().map(_(0)).toList
    primaryTypeValues.length mustEqual 2
    primaryTypeValues mustEqual expectedValues
  }

  "When calling extractDistinctValuesFromSingleColumn for primaryType column sorted ascending" must "return distinct values for the primaryType" in {
    val initialDF = chicagoCrimeDatasetExtractor.extractInitialDataset(createDataFrame())
    val distinctPrimaryTypeDataframe = chicagoCrimeDatasetExtractor.extractDistinctValuesFromSingleColumn("primaryType", initialDF, sorted = true)
    val primaryTypeValues = distinctPrimaryTypeDataframe.collect().map(_(0)).toList
    primaryTypeValues.length mustEqual 2
    primaryTypeValues.head mustEqual "DECEPTIVE PRACTICE"
    primaryTypeValues(1) mustEqual "THEFT"
  }

  "When calling extractDistinctValuesFromSingleColumn for primaryType column sorted descending" must "return distinct values for the primaryType" in {
    val initialDF = chicagoCrimeDatasetExtractor.extractInitialDataset(createDataFrame())
    val distinctPrimaryTypeDataframe = chicagoCrimeDatasetExtractor.extractDistinctValuesFromSingleColumn("primaryType", initialDF, sorted = true, isAscendingOrder = false)
    val primaryTypeValues = distinctPrimaryTypeDataframe.collect().map(_(0)).toList
    primaryTypeValues.length mustEqual 2
    primaryTypeValues.head mustEqual "THEFT"
    primaryTypeValues(1) mustEqual "DECEPTIVE PRACTICE"
  }

  "When counting crime numbers per primary type ascending order" must "return a valid dataframe" in {
    val initialDF = chicagoCrimeDatasetExtractor.extractInitialDataset(createDataFrame())
    val crimeCountPerPrimaryType = chicagoCrimeDatasetExtractor.countCrimeGroupedByColumn(initialDF, "primaryType")
    val results = crimeCountPerPrimaryType.collect().map(row => (row.getString(0), row.getLong(1))).toList
    results.length mustEqual 2

    results.head._1 mustEqual "THEFT"
    results.head._2 mustEqual 1

    results(1)._1 mustEqual "DECEPTIVE PRACTICE"
    results(1)._2 mustEqual 2
  }

  "When counting crime numbers per primary type descending order" must "return a valid dataframe" in {
    val initialDF = chicagoCrimeDatasetExtractor.extractInitialDataset(createDataFrame())
    val crimeCountPerPrimaryType = chicagoCrimeDatasetExtractor.countCrimeGroupedByColumn(initialDF, "primaryType", isSortedAscending = false)
    val results = crimeCountPerPrimaryType.collect().map(row => (row.getString(0), row.getLong(1))).toList
    results.length mustEqual 2

    results(1)._1 mustEqual "THEFT"
    results(1)._2 mustEqual 1

    results.head._1 mustEqual "DECEPTIVE PRACTICE"
    results.head._2 mustEqual 2
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
