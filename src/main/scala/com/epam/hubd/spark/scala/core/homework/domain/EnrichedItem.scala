package com.epam.hubd.spark.scala.core.homework.domain

import java.text.DecimalFormat
import java.math.RoundingMode

case class EnrichedItem(motelId: String, motelName: String, bidDate: String, loSa: String, price: Double) {
  
  // rounding makes a perfect head-ache in this problem as in the resulting files doubles are compared
  // as text, so we want to round
  // to the 3 decimal digits after the point
  // but removing the trailing zeroes
  // except one :)
  
  val decimalFormat : DecimalFormat = new DecimalFormat("0.0##")
  decimalFormat.setRoundingMode(RoundingMode.HALF_UP)
  
  override def toString: String = {
    val roundedPrice: String = decimalFormat.format(Math.round(price * 1000.0) / 1000.0)
    s"$motelId,$motelName,$bidDate,$loSa,$roundedPrice"
  }
}
