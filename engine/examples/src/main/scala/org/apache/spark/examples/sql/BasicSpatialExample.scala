/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dong on 1/20/16.
  */
object BasicSpatialExample {
  case class Point(name: String, x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDRangeQuery").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", 4.toString)

    import sqlContext.implicits._

    val pointsDataFrame = sc.textFile("./examples/src/main/resources/point1.txt").map(_.split(","))
      .map(p => Point(p(0), p(1).trim().toDouble, p(2).trim().toDouble)).toDF()
    pointsDataFrame.registerTempTable("points")
    val sqlQueryForRangeAndKNN =
      "SELECT * FROM points " +
        "WHERE y < 10 AND (POINT(x, y) IN KNN(POINT(4, 4), 3) " +
        "AND POINT(x, y) IN KNN(POINT(3, 3), 3)) " +
        "AND POINT(x, y) IN RANGE(POINT(3.5, 3.5), POINT(5.5, 5.5))" +
        "AND POINT(x, y) IN CIRCLERANGE(POINT(4.5, 4.5), 2)"
    val sqlQueryForCircleRange = "SELECT * FROM points " +
      "WHERE POINT(x, y) IN CIRCLERANGE(POINT(4.5, 4.5), 2)"

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