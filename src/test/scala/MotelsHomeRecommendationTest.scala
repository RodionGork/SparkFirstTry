package com.epam.hubd.spark.scala.sql.homework

import java.io.File

import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendationTest._
import org.apache.spark.sql.{Row, DataFrame, UserDefinedFunction}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}
import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.rules.TemporaryFolder

class MotelsHomeRecommendationTest {
  val _temporaryFolder = new TemporaryFolder
  import sqlContext.implicits._
  val dataFrameUtils = new DataFrameTestUtils(sc, sqlContext)

  @Rule
  def temporaryFolder = _temporaryFolder

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.gz.parquet"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/expected_sql"

  private var outputFolder: File = null

  @Before
  def setup() : Unit = {
    outputFolder = temporaryFolder.newFolder("output")
  }

  @Test
  def test_getErroneousRecords() = {
    val rawBids = Seq(
      ("1", "06-05-02-2016", "ERROR_1"),
      ("2", "15-04-08-2016", "0.89"),
      ("3", "07-05-02-2016", "ERROR_2"),
      ("4", "06-05-02-2016", "ERROR_1"),
      ("5", "06-05-02-2016", "ERROR_2")
    ).toDF("MotelID", "BidDate", "Error")
    
    val expected = Seq(
      ("06-05-02-2016", "ERROR_1", 2),
      ("06-05-02-2016", "ERROR_2", 1),
      ("07-05-02-2016", "ERROR_2", 1)
    ).toDF("BidDate", "Error", "count")

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)
    
    dataFrameUtils.assertEquals(expected, erroneousRecords)
  }
  
  @Test
  def test_getBidsSplit() = {
    val rawBids = Seq(
      ("1", "06-05-02-2016", "ERROR_1", "", "", ""),
      ("2", "15-04-08-2016", "0.89", "1.1", "1.2", "1.3"),
      ("3", "17-04-08-2016", "0.89", "", "3.142", "blaha")
    ).toDF("MotelID", "BidDate", "Error", "US", "MX", "CA")
    
    val expected = Seq(
      ("2", "15-04-08-2016", 1.1, "US"),
      ("2", "15-04-08-2016", 1.2, "MX"),
      ("2", "15-04-08-2016", 1.3, "CA"),
      ("3", "17-04-08-2016", 3.142, "MX")
    ).toDF("MotelID", "BidDate", "LoSa", "Price")
    
    val splitBids = MotelsHomeRecommendation.getBidsSplit(rawBids)
    dataFrameUtils.assertEquals(expected, splitBids)
  }
  
  @Test
  def test_getBidsUsd() = {
    val bids = Seq(
      ("2", "15-04-08-2016", 3.0, "US"),
      ("3", "17-04-08-2016", 2.0, "MX")
    ).toDF("MotelID", "BidDate", "Price", "LoSa")
    
    val rates = Seq(
      ("17-04-08-2016", 0.5),
      ("19-04-08-2016", 0.25)
    ).toDF("ExchDate", "Rate")
    
    val expected = Seq(
      ("3", "17-04-08-2016", 1.0, "MX")
    ).toDF("MotelID", "BidDate", "Price", "LoSa")
    
    val bidsUsd = MotelsHomeRecommendation.getBidsUsd(bids, rates)
    dataFrameUtils.assertEquals(expected, bidsUsd)
  }
  
  @Test
  def test_getMaxBids() = {
    // note, unlike in previous HW this one allows duplicates :(
    val bids = Seq(
      ("2", "15-04-08-2016", 3.0, "US"),
      ("2", "15-04-08-2016", 2.5, "MX"),
      ("2", "15-04-08-2016", 3.5, "CA"),
      ("3", "17-04-08-2016", 0.7, "US"),
      ("3", "17-04-08-2016", 1.5, "MX"),
      ("3", "17-04-08-2016", 1.0, "CA"),
      ("4", "19-04-08-2016", 1.0, "US"),
      ("4", "19-04-08-2016", 1.0, "MX"),
      ("4", "19-04-08-2016", 1.0, "CA")
    ).toDF("MotelID", "BidDate", "Price", "LoSa")
    
    val expected = Seq(
      ("2", "15-04-08-2016", 3.5, "CA"),
      ("3", "17-04-08-2016", 1.5, "MX"),
      ("4", "19-04-08-2016", 1.0, "US"),
      ("4", "19-04-08-2016", 1.0, "MX"),
      ("4", "19-04-08-2016", 1.0, "CA")
    ).toDF("MotelID", "BidDate", "Price", "LoSa")
    
    val maxBids = MotelsHomeRecommendation.getMaxBids(bids)
    dataFrameUtils.assertEquals(expected, maxBids)
  }
  
  @Test
  def test_getEnriched() = {
    val bids = Seq(
      ("2", "15-04-08-2016", 3.5, "CA"),
      ("3", "17-04-08-2016", 1.5, "MX"),
      ("2", "19-04-08-2016", 1.0, "US")
    ).toDF("MotelID", "BidDate", "Price", "LoSa")
    val motels = Seq(
      ("3", "Zanzibarro"),
      ("2", "Gays' lair")
    ).toDF("MotelID", "MotelName")
    
    val expected = Seq(
      ("2", "Gays' lair", "2016-08-04 15:00", "CA", 3.5),
      ("3", "Zanzibarro", "2016-08-04 17:00", "MX", 1.5),
      ("2", "Gays' lair", "2016-08-04 19:00", "US", 1.0)
    ).toDF("MotelID", "MotelName", "BidDate", "Price", "LoSa")
    
    val enriched = MotelsHomeRecommendation.getEnriched(bids, motels)
    dataFrameUtils.assertEquals(expected, enriched)
  }
  
  @Test
  def shouldFilterErrorsAndCreateCorrectAggregates() = {

    runIntegrationTest()

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  @After
  def teardown(): Unit = {
    //outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sqlContext, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RDDComparisons.assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }
  
  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}

object MotelsHomeRecommendationTest {
  val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test"))
  val sqlContext = new HiveContext(sc)

  @AfterClass
  def afterTests(): Unit = {
    sc.stop
  }
}
