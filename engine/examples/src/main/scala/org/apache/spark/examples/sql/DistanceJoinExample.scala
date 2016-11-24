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

// scalastyle:off println
package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dong on 1/21/16.
  * Distance Join Example
  */
object DistanceJoinExample {
  case class Point(name: String, x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DistanceJoinTest").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", 16.toString)
    sqlContext.setConf("spark.sql.sampleRate", 1.toString)
    sqlContext.setConf("spark.sql.joins.distanceJoin", "DJSpark")

    import sqlContext.implicits._

    val point1 = sc.textFile("./examples/src/main/resources/point1.txt").map(_.split(","))
      .map(p => Point(p(0), p(1).trim().toDouble, p(2).trim().toDouble)).toDF()
    val point2 = sc.textFile("./examples/src/main/resources/point3.txt").map(_.split(","))
      .map(p => Point(p(0), p(1).trim().toDouble, p(2).trim().toDouble)).toDF()
    point1.registerTempTable("point1")
    point2.registerTempTable("point2")

    val joinQuery = "SELECT * FROM point1 DISTANCE JOIN point2 " +
      "ON POINT(point2.x, point2.y) IN CIRCLERANGE(POINT(point1.x, point1.y), 3.0) WHERE point1.x < point2.x"
    val startTime1 = System.currentTimeMillis()
    val df = sqlContext.sql(joinQuery)
    df.collect().foreach(println)
    val endTime1 = System.currentTimeMillis()
    println("----------------------------")
    println("Time :  " + (endTime1 - startTime1) / 1000.0 + "s. ")
    sc.stop()
  }
}
