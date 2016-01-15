package org.apache.spark.examples.sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Dong on 4/27/2015.
 * Spatial Operations implemented by UDF
 */
object SQLSpatialExample {
  case class Point(name: String, x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UDFSpatialExample").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", 4.toString)

    import sqlContext.implicits._

    val point1 = sc.textFile("./examples/src/main/resources/point1.txt").map(_.split(",")).map(p => {
      Point(p(0), p(1).trim().toDouble, p(2).trim().toDouble)
    }).toDF()
    val point2 = sc.textFile("./examples/src/main/resources/point3.txt").map(_.split(",")).map(p => {
      Point(p(0), p(1).trim().toDouble, p(2).trim().toDouble)
    }).toDF()
    point1.registerTempTable("point1")
    point2.registerTempTable("point2")

    val sqlQueryForRangeAndKNN = "SELECT * FROM ((SELECT * FROM point1 WHERE x >= 8 AND x <= 9 AND y >= 1 AND y <= 5) " +
      "UNION DISTINCT (SELECT * FROM point1 ORDER BY (x - 3) * (x - 3) + (y - 3) * (y - 3) ASC LIMIT 3)) AS tmp WHERE tmp.y < 4.5"
    val sqlQueryForCircleRange = "SELECT * FROM point1 WHERE (x - 4.5) * (x - 4.5) + (y - 4.5) * (y - 4.5) <= 4"
    val distanceJoinQuery = "SELECT * FROM point1 LEFT OUTER JOIN point2 ON (point2.x - point1.x) * (point2.x - point1.x)" +
      " + (point2.y - point1.y) * (point2.y - point1.y) <= 16"
    //XX Can KNN-JOIN be expressed by standard SQL? (Even with UDF)
    //val knnJoinQuery = "SELECT * FROM point1, (SELECT * FROM point2 ORDER BY (point2.x - point1.x) *" +
    //  " (point2.x - point1.x) + (point2.y - point1.y) * (point2.y - point1.y) LIMIT 3) AS p2"

    println("****table content****")
    println("****table point1****")
    sqlContext.sql("SELECT * FROM point1").toDF().collect().foreach(println)
    println("****table point2****")
    sqlContext.sql("SELECT * FROM point2").toDF().collect().foreach(println)

    println("****Test for KNN and RangeQuery****")
    println("SQL query: " + sqlQueryForRangeAndKNN)
    sqlContext.sql(sqlQueryForRangeAndKNN).toDF().collect().foreach(println)
    println("****Test for CircleRange****")
    println("SQL query: " + sqlQueryForCircleRange)
    sqlContext.sql(sqlQueryForCircleRange).toDF().collect().foreach(println)
    println("****Test for Distance Join****")
    println("DISTANCE JOIN: " + distanceJoinQuery)
    sqlContext.sql(distanceJoinQuery).toDF().collect().foreach(println)
    println("****Test for KNN Join****")
    println("KNN JOIN: knn join on point1 and point2 with k = 3")

    val point1Array = sqlContext.sql("select * from point1").toDF().collect()

    def trans(name : String, x : Double, y : Double, k : Int) : String =
      f"select '$name%s', $x%.2f, $y%.2f, name, x, y" +
        f" from point2 ORDER BY (x  - $x%.2f) * (x - $x%.2f) * (y - $y%.2f) * (y - $y%.2f) ASC" +
        f" LIMIT $k%d"

    val head = point1Array.head
    var finalDF = sqlContext.sql(trans(head.getString(0), head.getDouble(1), head.getDouble(2), 3))

    for (i <- 1 to point1Array.length - 1) {
      val row = point1Array(i)
      val tmpname = row.getString(0)
      val tmpx = row.getDouble(1)
      val tmpy = row.getDouble(2)
      finalDF = finalDF.unionAll(sqlContext.sql(trans(tmpname, tmpx, tmpy, 3)).toDF())
    }

    finalDF.collect().foreach(println)
    sc.stop()
    println("Finished.")
  }
}
