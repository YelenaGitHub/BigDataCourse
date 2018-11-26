package com.epam.hubd.spark.scala.core.homework

import domain.{BidError, BidErrorCount, BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDateTime

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation")
    )

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")

  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    val rdd = sc.textFile(bidsPath)
    rdd
      .map(line => line.split(","))
      .map(line => line.toList)
  }

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    rawBids
      .filter(rddList => rddList.exists(listElement => listElement.startsWith("ERROR")))
      .map({
        case(element) => (BidError(element(1), element(2)), 1)
          })
      .reduceByKey((countElement1, countElement2) => countElement1 + countElement2)
      .map(element => BidErrorCount(element._1.date, element._1.errorMessage, element._2).toString)

  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    val rdd = sc.textFile(exchangeRatesPath)
    rdd
      .map(line => line.split(Constants.DELIMITER))
      .map(listElement => Map(listElement(0) -> listElement(3).toDouble))
      .flatMap(_.toMap)
      .collect()
      .toMap
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    rawBids
      .filter(itemList => itemList.size > 3)
      .map(element =>
        List(
          element(0), // motelID
          LocalDateTime.parse(element(1), Constants.INPUT_DATE_FORMAT).toString(Constants.INPUT_DATE_FORMAT), // bidDate
          if (element(5).isEmpty) "0" else element(5), // loSa US
          if (element(8).isEmpty) "0" else element(8), // loSa CA
          if (element(6).isEmpty) "0" else element(6))) // loSa MX

      .map(listElement => List(

      BidItem(listElement(0),
        listElement(1),
        Constants.TARGET_LOSAS(0),
        listElement(2).toDouble * exchangeRates(listElement(1))),

      BidItem(listElement(0),
        listElement(1),
        Constants.TARGET_LOSAS(1),
        listElement(3).toDouble * exchangeRates(listElement(1))),

      BidItem(listElement(0),
        listElement(1),
        Constants.TARGET_LOSAS(2),
        listElement(4).toDouble * exchangeRates(listElement(1)))
    ))
      .flatMap(_.toList)
      .filter(element => element.price > 0)
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    val rdd = sc.textFile(motelsPath)
    rdd
      .map { line =>
        var element = line.split(",")
        (element(0), element(1))
      }
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {

    bids
      .map(element => (element.motelId, element))
      .join(motels)
      .map({
        case (element1, element2) => ((element1, element2._1.bidDate), (element2._2, element2._1.loSa, element2._1.price))
      })
      // key (motelId, bidDate)
      // value (motelName, loSa, price)
      .reduceByKey({
      case ((motelName1, loSa1, price1), (motelName2, loSa2, price2)) =>
        if (price1 > price2)
          (motelName1, loSa1, price1)
        else
          (motelName2, loSa2, price2)
    })
      .map(element => EnrichedItem(
                        element._1._1,
                        element._2._1,
                        LocalDateTime.parse(element._1._2, Constants.INPUT_DATE_FORMAT).toString(Constants.OUTPUT_DATE_FORMAT),
                        element._2._2,
                        element._2._3))
  }

}
