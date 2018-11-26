package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.format.DateTimeFormat

object Constants {

  val DELIMITER = ","

  val CSV_FORMAT = "com.databricks.spark.csv"

  val EXCHANGE_RATES_HEADER = StructType(Array("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
    .map(field => StructField(field, StringType, true)))

  val TARGET_LOSAS = Seq("US", "CA", "MX")

  val DOUBLE_PRECISION = 3
  val ERROR_PREFIX = "ERROR"

  val INPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
}
