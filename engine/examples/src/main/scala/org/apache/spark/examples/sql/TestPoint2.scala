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
import scala.collection.mutable.ListBuffer
/**
  * Created by zhongpu on 16-7-14.
  */
object TestPoint2 {

  case class PointData(x: Double, y: Double)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("PointTest2").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.sampleRate", 1.toString)

    import sqlContext.implicits._

    var points = ListBuffer[PointData]()

    for (i <- 1 to 100) {
      points += PointData(i, i)
    }
    val rdd = sc.parallelize(points)
    rdd.toDF().registerTempTable("MyPoint")

    sqlContext.sql("CREATE INDEX treeMapIndex ON MyPoint (x, y) USE rtree")

    val sqlQuery = "SELECT x FROM MyPoint WHERE POINT(x, y) IN RANGE (POINT(5, 5), POINT(20, 20))"

    val df = sqlContext.sql(sqlQuery)
    println(df.queryExecution)

    df.show()

    sc.stop()
  }

}
