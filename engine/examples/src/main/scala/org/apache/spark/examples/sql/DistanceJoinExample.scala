package org.apache.spark.examples.sql

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by crystalove on 15-5-25.
 */
object DistanceJoinExample {
  case class Point(name: String, x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DistanceJoinOperationTestFull").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", 16.toString)
    sqlContext.setConf("spark.sql.sampleRate", 1.toString)
    sqlContext.setConf("spark.sql.joins.distanceJoinMethod", "RTreeDistanceJoin")

    import sqlContext.implicits._

    val point1 = sc.textFile("./examples/src/main/resources/point1.txt").map(_.split(",")).map(p => {
      Point(p(0), p(1).trim().toDouble, p(2).trim().toDouble)
    }).toDF()
    val point2 = sc.textFile("./examples/src/main/resources/point3.txt").map(_.split(",")).map(p => {
      Point(p(0), p(1).trim().toDouble, p(2).trim().toDouble)
    }).toDF()
    point1.registerTempTable("point1")
    point2.registerTempTable("point2")

    val joinQuery = "SELECT * FROM point1 DISTANCE JOIN point2 ON (POINT(point2.x, point2.y) IN CIRCLERANGE(POINT(point1.x, point1.y), 3.0))"
    val startTime1 = System.currentTimeMillis()
    sqlContext.sql(joinQuery).toDF().collect().foreach(println)
    val endTime1 = System.currentTimeMillis()
    println("----------------------------")
    println("Time :  " + (endTime1 - startTime1) / 1000.0 + "s. ")
    sc.stop()
  }
}
