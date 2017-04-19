package com.epam.hubd.spark.scala.sql.homework

import java.io.File

import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendationTest._
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import org.junit.rules.TemporaryFolder

/**
  * Created by Csaba_Bejan on 8/22/2016.
  */
class MotelsHomeRecommendationTest extends DataFrameSuiteBase {
  val _temporaryFolder = new TemporaryFolder

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
  def setup() = {
    outputFolder = temporaryFolder.newFolder("output")
  }

@Test
  def shouldCollectErroneousRecords() = {
    import sqlContext.implicits._
    val rawBids = sc.parallelize(Seq(
      List("1", "06-05-02-2016", "ERROR_1"),
      List("2", "15-04-08-2016", "0.89"),
      List("3", "07-05-02-2016", "ERROR_2"),
      List("4", "06-05-02-2016", "ERROR_1"),
      List("5", "06-05-02-2016", "ERROR_2")
    )).toDF

    val expected = sc.parallelize(Seq(
      List("06-05-02-2016,ERROR_1", "2"),
      List("06-05-02-2016,ERROR_2", "1"),
      List("07-05-02-2016,ERROR_2", "1")
    )).toDF

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    assertDataFrameEquals(expected, erroneousRecords)
  }

  //@Test
  def shouldFilterErrorsAndCreateCorrectAggregates() = {

    runIntegrationTest()

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  @After
  def teardown(): Unit = {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sqlContext, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    //RDDComparisons.assertRDDEquals(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}

object MotelsHomeRecommendationTest {
  var sc: SparkContext = null
  var sqlContext: HiveContext = null

  @BeforeClass
  def beforeTests() = {
    sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test"))
    sqlContext = new HiveContext(sc)
  }

  @AfterClass
  def afterTests() = {
    sc.stop
  }
}
