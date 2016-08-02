/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
// scalastyle:off println
package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

/**
  * Created by gefei on 16-6-21.
  */
object SpatialOperationExample {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SpatialOperationExample").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.index.threshold", 2.toString)
    //    sqlContext.setConf("spark.sql.partition.method", "KDTreeParitioner")
    //    sqlContext.setConf("spark.sql.partition.method", "QuadTreePartitioner")

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

    sqlContext.sql("CREATE INDEX pointIndex ON point1(x, y) USE rtree")
//    sqlContext.sql("CREATE INDEX treeMapIndex ON point2 (x) USE treemap")
    sqlContext.sql("SHOW INDEX ON point1")

    println("----------------------------")

//    val sqlQuery = "SELECT * FROM point1 " +
//      "WHERE POINT(point1.x, point1.y, point1.z) " +
//      "IN RANGE( POINT(100, 222, 222), POINT(300, 333, 333))" +
//      "ORDER BY x"
//    val sqlQuery =
//      "SELECT * FROM point1 WHERE x > 50 " +
//        "AND ((x < 52) OR (x <= 70 AND y < 80) OR (x > 80 AND x <= 100 AND y > 90)) ORDER BY y"
    val sqlQuery = "SELECT * FROM point1 WHERE POINT(x, y) IN KNN(POINT(50, 50), 201) " +
  "AND POINT(x, y) IN KNN(POINT(200, 200), 11) ORDER BY y"
//    val sqlQuery = "SELECT * FROM point1 WHERE POINT(x, y) IN RANGE(POINT(50, 50), POINT(100, 100)) ORDER BY y"
    val df = sqlContext.sql(sqlQuery)
    println(df.queryExecution)
    println("-------------------")

    df.rdd.collect().foreach(println)

    sc.stop()
  }
}
