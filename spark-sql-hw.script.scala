import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter

val bidsPath = "bids.gz.parquet"
val exchPath = "exchange_rate.txt"
val motelsPath = "motels.gz.parquet"

val rawBids = sqlContext.read.parquet(bidsPath).withColumnRenamed("HU", "Error").select("MotelId", "BidDate", "US", "MX", "CA", "Error")

val exchRate = sqlContext.read.format("csv").load(exchPath).select("C0", "C3").toDF("ExchDate", "Rate")

val filteredBids = rawBids.where(!col("Error").like("ERROR%"))

val bidsUS = filteredBids.select(col("MotelID"), col("BidDate"), col("US").alias("Price").cast(DoubleType)).withColumn("LoSa", lit("US"))
val bidsMX = filteredBids.select(col("MotelID"), col("BidDate"), col("MX").alias("Price").cast(DoubleType)).withColumn("LoSa", lit("MX"))
val bidsCA = filteredBids.select(col("MotelID"), col("BidDate"), col("CA").alias("Price").cast(DoubleType)).withColumn("LoSa", lit("CA"))

val bidsAll = bidsUS.unionAll(bidsMX).unionAll(bidsCA).where(col("Price").isNotNull)

val bidsAndRates = bidsAll.join(exchRate, bidsAll("BidDate") === exchRate("ExchDate"))

object DateFormats {
  val INPUT_DATE_FORMAT = DateTimeFormatter.ofPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
}

val dateFormatUdf = udf((s: String) => DateFormats.OUTPUT_DATE_FORMAT.format(DateFormats.INPUT_DATE_FORMAT.parse(s)))

val bidsUsd = bidsAndRates.withColumn("Price", col("Price") * col("Rate")).drop("ExchDate").drop("Rate").withColumn("BidDate", dateFormatUdf(col("BidDate")))

val bidsMax = bids.groupBy("MotelID", "BidDate").agg(max("Price").alias("Price"))

val bids = bids.join(bidsMax, Seq("MotelID", "BidDate", "Price"))

val motels = sqlContext.read.parquet(motelsPath).select("MotelID", "MotelName")

val enriched = bids.join(motels, "MotelID")

