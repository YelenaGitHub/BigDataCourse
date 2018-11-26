package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormatter

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf()
      .setAppName("motels-home-recommendation")
    )

    val sqlContext = new HiveContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = getConvertInputDate

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {

    sqlContext
      .read.parquet(bidsPath)
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {

    rawBids
      .select("BidDate", "HU")
      .withColumn("Error", rawBids.col("HU"))
      .filter(rawBids.col("HU").startsWith(Constants.ERROR_PREFIX))
      .groupBy("BidDate", "Error")
      .count()

  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {

    sqlContext
      .read
      .format(Constants.CSV_FORMAT)
      .option("delimiter", Constants.DELIMITER)
      .schema(Constants.EXCHANGE_RATES_HEADER)
      .load(exchangeRatesPath)
  }

  def getConvertInputDate: UserDefinedFunction = {
    udf((inputDate: String) =>
      convertDate(inputDate, Constants.INPUT_DATE_FORMAT, Constants.INPUT_DATE_FORMAT))
  }

  def getConvertOutputDate: UserDefinedFunction = {
    udf((inputDate: String) =>
      convertDate(inputDate, Constants.INPUT_DATE_FORMAT, Constants.OUTPUT_DATE_FORMAT))
  }

  def convertDate(d: String, inputFormat: DateTimeFormatter, outputFormat: DateTimeFormatter): String = {
    LocalDateTime.parse(d, inputFormat).toString(outputFormat)
  }

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {

    val resultDF = rawBids
      .select("MotelID", "BidDate", "US", "CA", "MX")

      .withColumn("CountryPrice",
          explode(array(
            array(lit("US"), rawBids.col("US")),
            array(lit("CA"), rawBids.col("CA")),
            array(lit("MX"), rawBids.col("MX"))
          )))

      .join(exchangeRates, rawBids.col("BidDate").equalTo(exchangeRates.col("ValidFrom")))
      .select("MotelID", "BidDate", "CountryPrice", "ExchangeRate")
      .withColumn("BidDate", getConvertInputDate(rawBids.col("BidDate")))

    resultDF
      .select(col("MotelID"),
              col("BidDate"),
              col("CountryPrice").getItem(0).alias("Country"),
             (round(col("CountryPrice").getItem(1) *
                    col("ExchangeRate"), Constants.DOUBLE_PRECISION)).alias("Price")
              )
      .filter(length(trim(col("Price"))) > 0)
  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {
    sqlContext
      .read.parquet(motelsPath)
      .select("MotelID", "MotelName")
  }

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {

    val window = Window.partitionBy(col("MotelID"), col("BidDate"))

    bids
      .join(motels, Seq("MotelID"))
      .select(col("MotelID"),
              col("MotelName"),
              getConvertOutputDate(col("BidDate")).alias("BidDate"),
              col("Country"),
              col("Price"),
              max(col("Price")).over(window).alias("MaxPrice"))
      .filter(col("Price").equalTo(col("MaxPrice")))
      .drop(col("MaxPrice"))
  }

}

