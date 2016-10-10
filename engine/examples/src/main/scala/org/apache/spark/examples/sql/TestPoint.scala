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

/**
  * Created by zhongpu on 16-7-11.
  */
object TestPoint {

  case class PointItem(id: Int, p: Point)
  case class PointCoord(x: Double, y: Double)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("PointTest").setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.sampleRate", 1.toString)

    import sqlContext.implicits._

    val rdd = sc.parallelize(1 to 100 map {i => PointItem(i, new Point(Array(i, i)))})
    rdd.toDF().registerTempTable("Point1")

    val rdd2 = sc.parallelize(1 to 100 map {i => PointItem(i, new Point(Array(i, i)))})
    rdd2.toDF().registerTempTable("Point2")

    val sqlQuery = "SELECT * FROM Point2"
    val df = sqlContext.sql(sqlQuery)
    println(df.queryExecution.executedPlan)
    df.show()

    val sqlQuery2 = "SELECT * FROM Point1 KNN JOIN Point2 ON" +
      " Point2.p IN KNN(Point1.p, 3)"
    val df2 = sqlContext.sql(sqlQuery2)
    println(df2.queryExecution.executedPlan)
    df2.show()

    sqlContext.sql("CREATE INDEX rIndex ON Point1 (p) USE rtree")
    sqlContext.sql("SHOW INDEX ON Point1")
    val sqlQuery3 = "SELECT p FROM Point1 WHERE p IN RANGE(POINT(8, 8), POINT(20, 20))"
    val df3 = sqlContext.sql(sqlQuery3)
    println(df3.queryExecution)
    df3.collect().foreach(println)

    val rdd3 = sc.parallelize(1 to 100 map {i => PointCoord(i, i)})
    rdd3.toDF().registerTempTable("Point3")
    sqlContext.sql("CREATE INDEX treapIndex ON Point3 (x) USE treap")
    val sqlQuery4 = "SELECT * FROM Point3 WHERE x > 90"
    val df4 = sqlContext.sql(sqlQuery4)
    println(df4.queryExecution)
    df4.show()

    sc.stop()

  }
}
