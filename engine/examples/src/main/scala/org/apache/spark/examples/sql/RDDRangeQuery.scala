package org.apache.spark.examples.sql

/**
 * Created by oizz01 on 15-4-8.
 * Range query examples.
 * Examples for range query, knn query and circle range.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object RDDRangeQuery {
  case class Point(name: String, x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDRangeQuery").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", 4.toString)

    import sqlContext.implicits._

    val pointsDataFrame = sc.textFile("./examples/src/main/resources/point1.txt").map(_.split(",")).map(p => {
      Point(p(0), p(1).trim().toDouble, p(2).trim().toDouble)
    }).toDF()
    pointsDataFrame.registerTempTable("points")
    val sqlQueryForRangeAndKNN =
      "SELECT * FROM points " +
      "WHERE y < 10 AND (POINT(x, y) IN KNN(POINT(4, 4), 3) AND POINT(x, y) IN KNN(POINT(3, 3), 3)) " +
                         "AND POINT(x, y) IN RANGE(POINT(3.5, 3.5), POINT(5.5, 5.5))" +
                         "AND POINT(x, y) IN CIRCLERANGE(POINT(4.5, 4.5), 2)"
    val sqlQueryForCircleRange = "SELECT * FROM points WHERE POINT(x, y) IN CIRCLERANGE(POINT(4.5, 4.5), 2)"

    println("****table content****")
    sqlContext.sql("SELECT * FROM points").toDF().collect().foreach(println)

    println("****Test for KNN and RangeQuery****")
    println("SQL query: " + sqlQueryForRangeAndKNN)
    sqlContext.sql(sqlQueryForRangeAndKNN).toDF().collect().foreach(println)
    println("****Test for CircleRange****")
    println("SQL query: " + sqlQueryForCircleRange)
    sqlContext.sql(sqlQueryForCircleRange).toDF().collect().foreach(println)

    sc.stop()
    println("Finished.")
  }
}
