# SparkFirstTry

First attemtps of writing in Scala for Apache Spark.

Using scala-maven-plugin to compile scala sources.

Running locally is possible with:

    bin/spark-submit --class com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation spark-core-hw.jar file:///.../bids.txt a.txt b.txt file:///.../errors.txt
