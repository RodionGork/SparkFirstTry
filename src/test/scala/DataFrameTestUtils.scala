package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType, StructField, AtomicType}
import com.holdenkarau.spark.testing.RDDComparisons

import org.scalatest.exceptions.TestFailedException
import org.junit.Assert

class DataFrameTestUtils(val sc: SparkContext, val sqlContext: HiveContext) {

  // I've found no convenient way to easily compare dataframes without converting them to rdds
  // the same seems to be advised on SO and other forums
  // It's a pity Spark API do not include such important test util functionality :(
  def assertEquals(expected: DataFrame, actual: DataFrame) = {
    val expectedRdd = expected.rdd
    val actualRdd = actual.rdd
    try {
      RDDComparisons.assertRDDEquals(expectedRdd, actualRdd)
    } catch {
      case _ : TestFailedException =>
        Assert.fail("DataFrames are not equal:\nexpected =\n"
            + expectedRdd.collect.toList
            + "\nactual =\n"
            + actualRdd.collect.toList)
    }
  }

}

