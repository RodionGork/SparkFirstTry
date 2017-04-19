package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions._
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
    val sqlContext = new HiveContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)
    
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")
/*
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    val convertDate: UserDefinedFunction = getConvertDate

    val bids: DataFrame = getBids(rawBids, exchangeRates)

    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
      */
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    if (bidsPath.endsWith(".parquet")) {
        sqlContext.read.parquet(bidsPath).withColumnRenamed("HU", "Error")
            .select("MotelId", "BidDate", "US", "MX", "CA", "Error")
    } else {
        sqlContext.read.format(Constants.CSV_FORMAT).load(bidsPath)
            .select("C0", "C1", "C5", "C6", "C8", "C2")
            .toDF("MotelId", "BidDate", "US", "MX", "CA", "Error")
    }
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    val errors = rawBids.select("BidDate", "Error").where(col("Error").like("ERROR%"))
    return errors.select(concat(col("BidDate"), lit(","), col("Error")).alias("dateAndError"))
        .groupBy("dateAndError").count()
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = ???

  def getConvertDate: UserDefinedFunction = ???

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = ???

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = ???

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = ???
}
