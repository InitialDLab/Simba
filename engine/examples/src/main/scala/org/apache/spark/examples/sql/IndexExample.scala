package org.apache.spark.examples.sql

import java.io.{File, PrintWriter}
import java.util.Scanner

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.index.RTreeType
import org.apache.spark.sql.index.TreeMapType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
 * Created by crystalove on 15-5-28.
 */
object IndexExample {
  case class PointData(x: Double, y: Double, z:Double, other: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("IndexTest").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.index.threshold", 2.toString)

    var leftData = ListBuffer[PointData]()
    var rightData = ListBuffer[PointData]()

    for (i <- 1 to 1000){
      leftData += PointData( i + 0.0, i + 0.0, i + 0.0, "a = " + i)
      rightData += PointData(i + 0.0, i + 0.0, i + 0.0, "a = " + (i + 1))
    }

    val leftRDD = sc.parallelize(leftData)
    val rightRDD = sc.parallelize(rightData)

    leftRDD.toDF().registerTempTable("point1")
    rightRDD.toDF().registerTempTable("point2")

    sqlContext.sql("CREATE INDEX pointIndex ON point1(x, y, z) USE rtree")
    sqlContext.sql("CREATE INDEX treeMapIndex ON point2 (x) USE treemap")
    sqlContext.sql("SHOW INDEX ON point1")

    println("----------------------------")

    val sqlQuery = "SELECT * FROM point1 where POINT(point1.x, point1.y, point1.z) in RANGE( POINT(100, 222, 222), POINT(300, 333, 333))"
    val df = sqlContext.sql(sqlQuery)

    df.collect().foreach(println)
    sc.stop()
  }
}

