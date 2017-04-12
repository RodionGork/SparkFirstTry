package com.epam.hubd.spark.scala.core.homework.domain

case class EnrichedItem(motelId: String, motelName: String, bidDate: String, loSa: String, price: Double) {

  override def toString: String = {
    val roundedPrice: String = "%.3f".format(price)
    s"$motelId,$motelName,$bidDate,$loSa,$roundedPrice"
  }
}
