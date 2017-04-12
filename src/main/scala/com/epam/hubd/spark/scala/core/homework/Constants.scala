package com.epam.hubd.spark.scala.core.homework

import java.text.SimpleDateFormat

object Constants {

  val DELIMITER = ","

  val BIDS_HEADER = Seq("MotelID", "BidDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE")
  val MOTELS_HEADER = Seq("MotelID", "MotelName", "Country", "URL", "Comment")
  val EXCHANGE_RATES_HEADER = Seq("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")

  val TARGET_LOSAS = Seq("US", "CA", "MX")

  val INPUT_DATE_FORMAT = new SimpleDateFormat("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm")
}
