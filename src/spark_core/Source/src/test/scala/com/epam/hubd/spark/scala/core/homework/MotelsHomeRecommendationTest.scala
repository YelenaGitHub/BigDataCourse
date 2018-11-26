package com.epam.hubd.spark.scala.core.homework

import java.io.File
import java.nio.file.Files

import com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR, getExchangeRates}
import com.epam.hubd.spark.scala.core.homework.domain.{BidItem, EnrichedItem}
import com.epam.hubd.spark.scala.core.util.RddComparator
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.junit._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION_SAMPLE = "src/test/resources/exchange_rate_sample.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read raw bids") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35", "1.63", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    )

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)

    assertRDDEquals(expected, rawBids)
  }

  test("should collect erroneous records") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    )

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    assertRDDEquals(expected, erroneousRecords)
  }

  test("should collect exchange rates") {

    val expectedExchangeRates = sc
      .parallelize(
        Map(
          "11-06-05-2016" -> 0.803,
          "11-05-08-2016" -> 0.873,
          "10-06-11-2015" -> 0.987,
          "10-05-02-2016" -> 0.876,
          "09-05-02-2016" -> 0.948)
          .toSeq)

    val exchangeRates = sc.parallelize(MotelsHomeRecommendation.getExchangeRates(sc, INPUT_EXCHANGE_RATES_INTEGRATION_SAMPLE).toSeq)

    assertRDDEquals(expectedExchangeRates, exchangeRates)

  }

  test("should collect bids") {

    val expectedBids = sc.parallelize(
      List(
        BidItem("0000002", "15-04-08-2016", "US", 20.7.toDouble),
        BidItem("0000002", "15-04-08-2016", "CA", 16.3.toDouble)
      )
    )

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)
    val exchangeRates = Map("15-04-08-2016" -> 10.toDouble,
                            "6-05-02-2016"  -> 20.toDouble)

    val bids : RDD[BidItem] = MotelsHomeRecommendation.getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double])

    assertRDDEquals(expectedBids.flatMap(_.toString), bids.flatMap(_.toString))
  }

  test("should collect motels' names") {

    val expectedMotels = sc
      .textFile(INPUT_MOTELS_INTEGRATION)
      .map(line => line.split(","))
      .map(line => (line(0) , line(1)))

    val motels : RDD[(String, String)] = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_INTEGRATION)

    assertRDDEquals(expectedMotels, motels)
  }

  test("should collect enriched items") {

    val expectedEnrichedItem = sc
      .parallelize(
        List(
          EnrichedItem("0000002", "Merlin Por Motel", "2016-08-04 15:00", "US", 20.7.toDouble)
        ))

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)
    val exchangeRates = Map("15-04-08-2016" -> 10.toDouble,
                            "6-05-02-2016"  -> 20.toDouble)

    val bids = MotelsHomeRecommendation.getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double])
    val motels = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_INTEGRATION)

    val enrichedItem : RDD[EnrichedItem] = MotelsHomeRecommendation.getEnriched(bids, motels)

    assertRDDEquals(expectedEnrichedItem.flatMap(_.toString), enrichedItem.flatMap(_.toString))
  }

  test("should filter errors and create correct aggregates") {

    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    //printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    //printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  after {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath).map(e => e(0) + "," + e(1) + "," + e(2) + "," + e(4))).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath).map(e => e(0) + "," + e(1) + "," + e(2) + "," + e(4))).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }
  
  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
