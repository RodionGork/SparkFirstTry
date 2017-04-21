package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
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
    
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    val bids: DataFrame = getBids(rawBids, exchangeRates)
    
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    val enriched: DataFrame = getEnriched(bids, motels)
    
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    if (bidsPath.endsWith(".parquet")) {
        sqlContext.read.parquet(bidsPath).withColumnRenamed("HU", "Error")
            .select("MotelID", "BidDate", "US", "MX", "CA", "Error")
    } else {
        sqlContext.read.format(Constants.CSV_FORMAT).load(bidsPath)
            .select("C0", "C1", "C5", "C6", "C8", "C2")
            .toDF("MotelID", "BidDate", "US", "MX", "CA", "Error")
    }
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    val errors = rawBids.select("BidDate", "Error").where(col("Error").like("ERROR%"))
    errors.select(concat(col("BidDate"), lit(","), col("Error")).alias("dateAndError"))
        .groupBy("dateAndError").count()
  }
  
  def getBidsSplit(rawBids: DataFrame): DataFrame = {
    val filteredBids = rawBids.where(!col("Error").like("ERROR%"))
    val bidsUS = filteredBids.select(col("MotelID"), col("BidDate"), col("US").alias("Price").cast(DoubleType)).withColumn("LoSa", lit("US"))
    val bidsMX = filteredBids.select(col("MotelID"), col("BidDate"), col("MX").alias("Price").cast(DoubleType)).withColumn("LoSa", lit("MX"))
    val bidsCA = filteredBids.select(col("MotelID"), col("BidDate"), col("CA").alias("Price").cast(DoubleType)).withColumn("LoSa", lit("CA"))

    bidsUS.unionAll(bidsMX).unionAll(bidsCA).where(col("Price").isNotNull)
  }
  
  def getExchangeRates(sqlContext: HiveContext, exchPath: String): DataFrame = {
    sqlContext.read.format(Constants.CSV_FORMAT).load(exchPath)
            .select("C0", "C3")
            .toDF("ExchDate", "Rate")
  }

  def getBidsUsd(bids: DataFrame, exchRate: DataFrame): DataFrame = {
    val bidsAndRates = bids.join(exchRate, bids("BidDate") === exchRate("ExchDate"))
    bidsAndRates.withColumn("Price", col("Price") * col("Rate")).drop("ExchDate").drop("Rate")
  }
  
  def getMaxBids(bids: DataFrame): DataFrame = {
    val bidsMax = bids.groupBy("MotelID", "BidDate").agg(max("Price").alias("Price"))
    bids.join(bidsMax, Seq("MotelID", "BidDate", "Price"))
  }
  
  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    val bidsSplit = getBidsSplit(rawBids)
    val bidsUsd = getBidsUsd(bidsSplit, exchangeRates)
    getMaxBids(bidsUsd)
  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {
    if (motelsPath.endsWith(".parquet")) {
        sqlContext.read.parquet(motelsPath).select("MotelID", "MotelName")
    } else {
        sqlContext.read.format(Constants.CSV_FORMAT).load(motelsPath)
            .select("C0", "C1").toDF("MotelID", "MotelName")
    }
  }

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {
    val dateFormatUdf = udf((s: String) =>
        Constants.OUTPUT_DATE_FORMAT.format(Constants.INPUT_DATE_FORMAT.parse(s)))
    bids.join(motels, "MotelID").withColumn("BidDate", dateFormatUdf(col("BidDate")))
  }
}
