package com.epam.hubd.spark.scala.core.homework.domain

case class BidErrorCount(date: String, errorMessage: String, errorCount: Int) {

  override def toString: String = s"$date,$errorMessage,$errorCount"

}
