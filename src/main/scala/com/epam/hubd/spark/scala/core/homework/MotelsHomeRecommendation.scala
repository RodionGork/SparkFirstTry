package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.domain.{BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)
    getErroneousRecords(rawBids).saveAsTextFile(outputBasePath + ".err")
    val exch = getExchangeRates(sc, exchangeRatesPath)
    val bids = getBids(rawBids, exch)
    val motels = getMotels(sc, motelsPath)
    val enriched = getEnriched(bids, motels)
    enriched.saveAsTextFile(outputBasePath + ".rich")
  }

  def processData1(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    val enriched:RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    val lines = sc.textFile(bidsPath)
    val records = lines.map(line => line.split(",").map(_.trim).toList)
    return records.filter(_.size >= 3)
  }

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    val errors = rawBids.filter(isErroneousRecord(_))
        .map(bid => (bid(1) + "," + bid(2), 1))
    val counts = errors.reduceByKey((x, y) => x + y)
    return counts.map(kv => kv._1 + "," + kv._2)
  }
  
  def isErroneousRecord(bid: List[String]) = bid(2).matches("ERROR\\_.*")
  
  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    val lines = sc.textFile(exchangeRatesPath).map(_.split(",").map(_.trim))
    val array = lines.map(line => (line(0), line(3).toDouble)).collect
    return array.toMap
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    val bidsByLosa = rawBids.filter(!isErroneousRecord(_)).flatMap(
            b => List((b(0), b(1), "US", b(5)), (b(0), b(1), "MX", b(6)), (b(0), b(1), "CA", b(8))))
    val bidsFiltered = bidsByLosa
            .map(b => (b._1, b._2, b._3, toDoubleWithNan(b._4) * exchangeRates.getOrElse(b._2, Double.NaN)))
            .filter(!_._4.isNaN)
    return bidsFiltered.map(b => BidItem(b._1, reformatDate(b._2), b._3, b._4))
  }
  
  def toDoubleWithNan(v: String): Double = {
    try {
      return v.toDouble
    } catch {
      case _ : NumberFormatException => Double.NaN
    }
  }
  
  def reformatDate(v: String): String = {
    return Constants.OUTPUT_DATE_FORMAT.format(Constants.INPUT_DATE_FORMAT.parse(v))
  }

  def getMotels(sc:SparkContext, motelsPath: String): RDD[(String, String)] = {
    val lines = sc.textFile(motelsPath)
    val records = lines.map(line => {
      val parts = line.split("\\,")
      (parts(0), parts(1))
    })
    return records
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val allEnriched = bids.map(bi => (bi.motelId, bi)).join(motels.map(m => (m._1, m)))
        .map(bm => EnrichedItem(bm._2._2._2, bm._1, bm._2._1.bidDate, bm._2._1.loSa, bm._2._1.price))
    val keyed = allEnriched.map(ei => (ei.motelId, ei.bidDate) -> ei)
    return keyed.reduceByKey((u, v) => if (u.price > v.price) u else v).map(_._2)
  }
  
}
