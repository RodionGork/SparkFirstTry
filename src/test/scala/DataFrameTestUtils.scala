package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType, StructField, AtomicType}
import com.holdenkarau.spark.testing.RDDComparisons

import org.scalatest.exceptions.TestFailedException

/*
I spent the whole day to come up with idea of how to:
  - create dataframe from collection for test purposes
  - compare two dataframes
And at the end it seems to me that there are no convenient way without
  converting them to / from RDDs as below

It seems a pity that test utilities are not provided by spark out of the box :(
*/

class DataFrameTestUtils(val sc: SparkContext, val sqlContext: HiveContext) {

  def createDataFrame(seq: Seq[Row], schema: Seq[(String, AtomicType with Product with Serializable, Boolean)]) = {
    sqlContext.createDataFrame(sc.parallelize(seq),
        StructType(schema.map(x => StructField(x._1, x._2, x._3))))
  }

  def assertEquals(expected: DataFrame, actual: DataFrame) = {
    val expectedRdd = expected.rdd
    val actualRdd = actual.rdd
    try {
      RDDComparisons.assertRDDEquals(expectedRdd, actualRdd)
    } catch {
      case _ : TestFailedException =>
        throw new RuntimeException("DataFrames are not equal:\nexpected =\n"
            + expectedRdd.collect.toList
            + "\nactual =\n"
            + actualRdd.collect.toList)
    }
  }

}

