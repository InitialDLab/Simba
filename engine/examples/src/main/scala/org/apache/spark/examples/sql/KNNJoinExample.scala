/*
 *  Copyright 2016 by Simba Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.examples.sql

import java.io.{PrintWriter, File}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Dong Xie on 10/2/2015.
 * KNN Join Example
 */
object KNNJoinExample {
  case class Point(id: String, x: Double, y: Double, info1: String, info2: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KNNJoinTest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", 16.toString)
    sqlContext.setConf("spark.sql.sampleRate", 1.toString)
    sqlContext.setConf("spark.sql.joins.knnJoinMethod", "NestedLoopKNNJoin")

    import sqlContext.implicits._

    //register table
    val point1 = sc.textFile("e:/osm-100k-left").map(_.split("\t")).map(p => {
      new Point(p(0), p(1).trim.toDouble, p(2).trim.toDouble, p(3), p(4))
    }).toDF()
    val point2 = sc.textFile("e:/osm-100k-right").map(_.split("\t")).map(p => {
      new Point(p(0), p(1).trim.toDouble, p(2).trim.toDouble, p(3), p(4))
    }).toDF()

    point1.registerTempTable("point1")
    point2.registerTempTable("point2")

    //knn join, implemented
    val knnJoinQuery = "SELECT * FROM point1 KNN JOIN point2 " +
                       "ON POINT(point2.x, point2.y) IN KNN(POINT(point1.x, point1.y), 3)"
    println("KNN JOIN: " + knnJoinQuery)
    val ans = sqlContext.sql(knnJoinQuery).toDF().collect()//.foreach(println)
    val pw = new PrintWriter(new File("e:/ans_check.txt"))
    ans.foreach(pw.println)
    println(ans.length)
    sc.stop()
  }
}
