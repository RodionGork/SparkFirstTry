package com.epam.hubd.spark.scala.core.util

import org.apache.spark.rdd.RDD
import com.holdenkarau.spark.testing.RDDComparisons

import org.scalatest.exceptions.TestFailedException

import scala.reflect.ClassTag

/**
  * Created by Csaba_Bejan on 8/30/2016.
  */
object RddComparator {

  def printDiff(expected: RDD[String], actual: RDD[String]) = {
    val actualArray = actual.collect
    val expectedArray = expected.collect
    val expectedDiff = expectedArray.filter(x => !actualArray.contains(x)).mkString("\n")
    val actualDiff = actualArray.filter(x=> !expectedArray.contains(x)).mkString("\n")
    if (!expectedDiff.isEmpty || !actualDiff.isEmpty) {
      println("")
      println("EXPECTED elements NOT available in actual set")
      println(expectedArray.filter(x => !actualArray.contains(x)).mkString("\n"))
      println("---")
      println("ACTUAL elements NOT available in expected set")
      println(actualArray.filter(x=> !expectedArray.contains(x)).mkString("\n"))
    } else {
      println("There were no differences between expected and actual RDDs")
    }
  }
  
  def assertEquals[T: ClassTag](expected: RDD[T], actual: RDD[T]) = {
    try {
      RDDComparisons.assertRDDEquals(expected, actual)
    } catch {
      case _ : TestFailedException =>
        throw new RuntimeException("RDDs are not equal:\nexpected =\n"
            + expected.collect.toList
            + "\nactual =\n"
            + actual.collect.toList)
    }
  }
  
}
