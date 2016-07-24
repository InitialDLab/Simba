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

import org.apache.spark.sql.{Point, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dong on 1/20/16.
  * DataFrame API Usage Example
  */
object DataFrameAPIExample {
  case class PointData(name: String, x: Double, y: Double, z: Double)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameApiExample").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", 4.toString)
    sqlContext.setConf("spark.sql.sampleRate", 1.toString)
    sqlContext.setConf("spark.sql.partitioner.strTransferThreshold", 1000000.toString)

    import sqlContext.implicits._

    val point1 = sc.parallelize((1 to 1000).map(x => PointData(x.toString, x, x, x))).toDF()
    val point2 = sc.parallelize((1 to 1000).map(x => PointData((x + 1).toString, x, x, x))).toDF()

    point1.registerTempTable("point1")
    point2.registerTempTable("point2")

    val df1 = sqlContext.sql("SELECT * FROM point1")
    val df2 = sqlContext.sql("SELECT * FROM point2")

    df1.range(Point(point1("x"), point1("y"), point1("z")), Point(1.0, 1.0, 1.0),
      Point(5.0, 5.0, 5.0)).collect().foreach(println)
    df1.range(Array("x", "y", "z"), Array(1.0, 1.0, 1.0), Array(1.0, 1.0, 1.0))
      .collect().foreach(println)

    df1.circleRange(Array("x", "y"), Array(3.0, 3.0), 3).collect().foreach(println)
    df1.circleRange(Point(point1("x"), point1("y")), Point(3.0, 3.0), 1.6)
      .collect().foreach(println)

    df1.range(Array("x", "y"), Array(1.0, 1.0), Array(3.0, 3.0)).collect().foreach(println)
    df1.range(Point(point1("x"), point1("y")), Point(1.0, 1.0), Point(3.0, 3.0))
      .collect().foreach(println)

    df1.knn(Array("x", "y"), Array(3.0, 3.0), 3).collect().foreach(println)
    df1.knn(Point(point1("x"), point1("y")), Point(3.0, 3.0), 3).collect().foreach(println)

    df1.distanceJoin(df2, Point(point1("x"), point1("y")),
      Point(point2("x"), point2("y")), 3).collect().foreach(println)
    df1.distanceJoin(df2, Array("x", "y"), Array("x", "y"), 3).collect().foreach(println)

    df1.knnJoin(df2, Point(point1("x"), point1("y")), Point(point2("x"), point2("y")), 3)
      .collect().foreach(println)
    df1.knnJoin(df2, Array("x", "y"), Array("x", "y"), 3).collect().foreach(println)
    df1.range(Point(point1("x"), point1("y"), point1("z")), Point(1.0, 1.0, 1.0),
      Point(5.0, 5.0, 5.0)).collect().foreach(println)
    df1.range(Array("x", "y", "z"), Array(1.0, 1.0, 1.0), Array(1.0, 1.0, 1.0))
      .collect().foreach(println)

    df1.circleRange(Array("x", "y"), Array(3.0, 3.0), 3).collect().foreach(println)
    df1.circleRange(Point(point1("x"), point1("y")), Point(3.0, 3.0), 1.6)
      .collect().foreach(println)

    df1.range(Array("x", "y"), Array(1.0, 1.0), Array(3.0, 3.0)).collect().foreach(println)
    df1.range(Point(point1("x"), point1("y")), Point(1.0, 1.0), Point(3.0, 3.0))
      .collect().foreach(println)

    df1.knn(Array("x", "y"), Array(3.0, 3.0), 3).collect().foreach(println)
    df1.knn(Point(point1("x"), point1("y")), Point(3.0, 3.0), 3).collect().foreach(println)

    df1.distanceJoin(df2, Point(point1("x"), point1("y")),
      Point(point2("x"), point2("y")), 3).collect().foreach(println)
    df1.distanceJoin(df2, Array("x", "y"), Array("x", "y"), 3).collect().foreach(println)

    df1.knnJoin(df2, Point(point1("x"), point1("y")), Point(point2("x"), point2("y")), 3)
      .collect().foreach(println)
    df1.knnJoin(df2, Array("x", "y"), Array("x", "y"), 3).collect().foreach(println)

    sc.stop()
    println("Finished.")
  }
}
