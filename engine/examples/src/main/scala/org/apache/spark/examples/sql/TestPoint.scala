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
import org.apache.spark.sql.spatial.Point

import scala.collection.mutable.ListBuffer
/**
  * Created by zhongpu on 16-7-11.
  */
object TestPoint {

  case class PointItem(id: Int, p: Point)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("PointTest").setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.sampleRate", 1.toString)

    import sqlContext.implicits._

    var points = ListBuffer[PointItem]()

    val points2 = ListBuffer[PointItem]()

    for (i <- 1 to 100) {
      val p = new Point(Array(i, i))
      points += PointItem(i, p)
      points2 += PointItem(i, p)
    }

    val rdd = sc.parallelize(points)
    val rdd2 = sc.parallelize(points2)

    rdd.toDF().registerTempTable("Point1")
    rdd2.toDF().registerTempTable("Point2")

    sqlContext.sql("CREATE INDEX rIndex ON Point1 (p) USE rtree")
    sqlContext.sql("SHOW INDEX ON Point1")

    val sqlQuery = "SELECT * FROM Point1 WHERE p IN RANGE(POINT(8, 8), POINT(20, 20))"
    val df = sqlContext.sql(sqlQuery)
    println(df.queryExecution)
    df.show()

    val sqlQuery2 = "SELECT * FROM Point2 WHERE p IN KNN (POINT(8, 8), 9)"
    val df2 = sqlContext.sql(sqlQuery2)
    println(df2.queryExecution)
    df.show()

    sc.stop()

  }
}
