package org.codecraftlabs.spark.chicagocrime

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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
      Row("account001", "Mike", 1),
      Row("account002", "Bob", 2),
      Row("account003", "Bob", 2)
    )
    val schema = schemaDefinition()
    sparkSession.get.createDataFrame(sparkSession.get.sparkContext.parallelize(sampleData), schema)
  }

  private def schemaDefinition(): StructType = {
    val accountName         = StructField("accountName", StringType, nullable = false)
    val userName            = StructField("userName", StringType, nullable = false)
    val userId              = StructField("userId", IntegerType, nullable = false)

    val fields = List(accountName, userName, userId)
    StructType(fields)
  }

}
