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

  case class PointItem(id: Int, p: Point) {
    override def toString : String = id.toString + " " + p.toString
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("PointTest").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    var points = ListBuffer[PointItem]()

    for (i <- 1 to 100) {
      val p = new Point(Array(i, i))
      points += PointItem(i, p)
    }

    val rdd = sc.parallelize(points)
    rdd.toDF().registerTempTable("Mypoint")

    val sqlQuery = "SELECT p FROM Mypoint WHERE p IN KNN(point(1, 5), 5)"
    val df = sqlContext.sql(sqlQuery)
    println(df.queryExecution)

    df.collect().foreach(println)
    df.collect().foreach(x => println(x.getClass))
    sc.stop()

  }
}
