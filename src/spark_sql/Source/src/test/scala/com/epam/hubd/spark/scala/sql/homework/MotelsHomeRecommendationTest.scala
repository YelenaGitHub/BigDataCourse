package com.epam.hubd.spark.scala.sql.homework

import java.io.File
import java.nio.file.Files

import com.epam.hubd.spark.scala.sql.util.RddComparator
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation._
import com.holdenkarau.spark.testing._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by Csaba_Bejan on 8/22/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase with BeforeAndAfter {
  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder = _temporaryFolder

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.gz.parquet"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/expected_sql"

  private var outputFolder: File = null
  var hiveContext: HiveContext = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
    hiveContext = new HiveContext(sc)
  }

  test ("should filter errors and create correct aggregates") {

    runIntegrationTest()

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  after {
    outputFolder.delete
  }

  test("should read raw bids") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val expected = sc.parallelize(
      List(
        ("0000002", "11-05-08-2016", "0.92", "1.68", "0.81", "0.68", "1.59", "", "1.63", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01"),
        ("0000006", "08-05-08-2016", "1.35", "", "1.13", "2.02", "1.33", "", "1.64", "1.70", "0.45", "2.02", "1.87", "1.75", "0.45", "1.28", "1.15", ""),
        ("0000003", "07-05-08-2016", "0.80", "1.05", "1.49", "0.43", "1.63", "0.42", "", "1.44", "1.73", "1.99", "1.86", "1.99", "0.97", "0.93", "", "0.51"),
        ("0000008", "07-05-08-2016", "1.43", "1.29", "1.22", "1.85", "1.26", "1.49", "1.26", "0.62", "1.56", "1.66", "0.67", "1.00", "", "1.05", "1.37", ""),
        ("0000008", "06-05-08-2016", "1.90", "0.99", "1.60", "1.31", "0.97", "0.39", "0.86", "1.59", "1.18", "", "1.19", "1.05", "1.64", "1.11", "1.57", "0.54"),
        ("0000009", "04-05-08-2016", "0.34", "0.62", "1.96", "0.62", "1.97", "", "0.96", "1.98", "2.08", "", "1.52", "0.66", "", "1.32", "1.44", "0.76"),
        ("0000006", "02-05-08-2016", "1.40", "0.86", "", "0.93", "0.42", "0.79", "1.95", "1.37", "0.43", "1.00", "1.42", "1.18", "0.87", "0.69", "1.24", "0.76"),
        ("0000008", "23-04-08-2016", "1.65", "0.91", "0.55", "0.76", "", "0.63", "0.44", "1.62", "1.19", "0.72", "2.06", "", "1.42", "", "0.53", "1.93"),
        ("0000001", "22-04-08-2016", "ERROR_INCONSISTENT_DATA", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      )
    ).toDF("MotelID", "BidDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE")
      // set 0 values instead of nulls, due to unavailability of testing framework compare nulls in data frames
     .na.fill("0")

    val actual: DataFrame = MotelsHomeRecommendation
      .getRawBids(hiveContext, INPUT_BIDS_INTEGRATION)
      .limit(9)
      // set 0 values instead of nulls, due to unavailability of testing framework compare nulls in data frames
      .na.fill("0")

      assertDataFrameEquals(expected, actual)
  }

  test("should collect erroneous records") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val rawBids = sc.parallelize(
      List(
        ("0000002", "11-05-08-2016", "0.92", "1.68", "0.81", "0.68", "1.59", "", "1.63", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01"),
        ("0000008", "23-04-08-2016", "1.65", "0.91", "0.55", "0.76", "", "0.63", "0.44", "1.62", "1.19", "0.72", "2.06", "", "1.42", "", "0.53", "1.93"),
        ("0000001", "22-04-08-2016", "ERROR_INCONSISTENT_DATA", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
        ("0000006", "08-05-08-2016", "1.35", "", "1.13", "2.02", "1.33", "", "1.64", "1.70", "0.45", "2.02", "1.87", "1.75", "0.45", "1.28", "1.15", ""),
        ("0000001", "22-04-08-2016", "ERROR_INCONSISTENT_DATA", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
        ("0000014", "23-04-08-2016", "ERROR_ACCESS_DENIED", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"),
        ("0000008", "23-04-08-2019", "1.65", "0.91", "0.55", "0.76", "", "0.63", "0.44", "1.62", "1.19", "0.72", "2.06", "", "1.42", "", "0.53", "1.93")
      )
    ).toDF("MotelID", "BidDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE")
     .na.fill("0")

    val expectedSchema = StructType(
      List(StructField("BidDate", StringType, false),
           StructField("Error", StringType, false),
           StructField("count", LongType, false)
      ))

    val expectedErroneousRecords = sc.parallelize(
      List[(String, String, Long)](
        ("23-04-08-2016", "ERROR_ACCESS_DENIED", 1),
        ("22-04-08-2016", "ERROR_INCONSISTENT_DATA", 2)
      )
    ).toDF("BidDate", "Error", "count")

    // set schema with all non null values
    val expectedWithUpdatedSchema = expectedErroneousRecords.sqlContext.createDataFrame(expectedErroneousRecords.rdd, expectedSchema)

    val actual: DataFrame = MotelsHomeRecommendation
      .getErroneousRecords(rawBids)
      .na.fill("0")

    assertDataFrameEquals(expectedWithUpdatedSchema, actual)
  }

  test("should collect exchange rates") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val expected = sc.parallelize(
      List(
        ("11-06-05-2016","Euro","EUR","0.803"),
        ("11-05-08-2016","Euro","EUR","0.873"),
        ("10-06-11-2015","Euro","EUR","0.987"),
        ("10-05-02-2016","Euro","EUR","0.876"),
        ("09-05-02-2016","Euro","EUR","0.948")
      )
    ).toDF("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")

    val actual: DataFrame = MotelsHomeRecommendation
      .getExchangeRates(hiveContext, INPUT_EXCHANGE_RATES_INTEGRATION)
      .limit(5)

    assertDataFrameEquals(expected, actual)
  }

  test("should collect bids"){

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val expectedSchema = StructType(
      List(
        StructField("MotelID", StringType, false),
        StructField("BidDate", StringType, true),
        StructField("Country", StringType, true),
        StructField("Price",   DoubleType, true)
      ))

    val expectedBids = sc.parallelize(
      List[(String, String, String, Double)](
        ("0000002", "11-05-08-2016", "US", 0.594),
        ("0000002", "11-05-08-2016", "CA", 1.423),
        ("0000002", "11-05-08-2016", "MX", 1.388),
        ("0000008", "23-04-08-2016", "US", 0.750),
        ("0000008", "23-04-08-2016", "CA", 0.434)
      )
    ).toDF("MotelID", "BidDate", "Country", "Price")

    val expectedWithUpdatedSchema = expectedBids.sqlContext.createDataFrame(expectedBids.rdd, expectedSchema)

    val rawBids = sc.parallelize(
      List(
        ("0000002", "11-05-08-2016", "0.92", "1.68", "0.81", "0.68", "1.59", "", "1.63", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01"),
        ("0000008", "23-04-08-2016", "1.65", "0.91", "0.55", "0.76", "", "0.63", "0.44", "1.62", "1.19", "0.72", "2.06", "", "1.42", "", "0.53", "1.93")
      )
    ).toDF("MotelID", "BidDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE")
      .na.fill("0")

    val exchangeRates = sc.parallelize(
      List(
        ("11-06-05-2016","Euro","EUR","0.803"),
        ("11-05-08-2016","Euro","EUR","0.873"),
        ("23-04-08-2016","Euro","EUR","0.987"),
        ("10-05-02-2016","Euro","EUR","0.876"),
        ("09-05-02-2016","Euro","EUR","0.948")
      )
    ).toDF("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")

    val actual: DataFrame = MotelsHomeRecommendation
      .getBids(rawBids, exchangeRates)

    assertDataFrameEquals(expectedWithUpdatedSchema, actual)

  }

  test("should collect enriched items") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val expected = sc.parallelize(
      List[(String, String, String, String, Double)] (
        ("0000001", "Grand Mengo Casino", "2016-08-05 11:00", "US", 2.388),
        ("0000001", "Grand Mengo Casino", "2016-08-05 11:00", "MX", 2.388),
        ("0000002", "Novelo Granja", "2016-08-04 23:00", "US", 0.75)
      )
    ).toDF("MotelID", "MotelName", "BidDate", "Country", "Price")

    val bids = sc.parallelize(
      List[(String, String, String, Double)] (
        ("0000001", "11-05-08-2016", "US", 2.388),
        ("0000001", "11-05-08-2016", "CA", 1.423),
        ("0000001", "11-05-08-2016", "MX", 2.388),
        ("0000002", "23-04-08-2016", "US", 0.750),
        ("0000002", "23-04-08-2016", "CA", 0.434)
      )
    ).toDF("MotelID", "BidDate", "Country", "Price")

    val motels = sc.parallelize(
      List(
        ("0000001","Grand Mengo Casino","HU","http://www.motels.home/?partnerID=7263fed9-8e16-4fba-8120-b24322f9a164","Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\n0000002,Novelo Granja,I,http://www.motels.home/?partnerID=e67f1f40-8734-43c4-a56d-61db67c7156e,Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."),
        ("0000002","Novelo Granja","I","http://www.motels.home/?partnerID=e67f1f40-8734-43c4-a56d-61db67c7156e","Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")
      )
    ).toDF("MotelID", "MotelName", "MotelCountry", "URL", "Comment")

    val actual: DataFrame = MotelsHomeRecommendation
      .getEnriched(bids, motels)

    val expectedWithUpdatedSchema = expected.sqlContext.createDataFrame(expected.rdd, actual.schema)

    assertDataFrameEquals(
      expectedWithUpdatedSchema.orderBy("MotelID", "BidDate", "Country"),
      actual.orderBy("MotelID", "BidDate", "Country"))
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(hiveContext, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }

}

object MotelsHomeRecommendationTest {
  var sc: SparkContext = null

  @BeforeClass
  def beforeTests() = {
    sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test"))
  }

  @AfterClass
  def afterTests() = {
    sc.stop
  }

}
