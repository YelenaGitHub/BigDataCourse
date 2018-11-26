package com.epam.hubd.spark.scala.core.homework.domain

import com.epam.hubd.spark.scala.core.homework.Constants

case class BidItem(motelId: String, bidDate: String, loSa: String, price: Double) {

  override def toString: String = {
    val formattedPrice = BigDecimal(price).setScale(Constants.DOUBLE_DECIMAL_PRECISION, BigDecimal.RoundingMode.HALF_UP).toDouble
    s"$motelId,$bidDate,$loSa,$formattedPrice"
  }

}
