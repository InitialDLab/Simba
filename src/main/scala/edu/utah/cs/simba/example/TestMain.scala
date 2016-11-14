/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.utah.cs.simba.example

import edu.utah.cs.simba.SimbaContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

/**
  * Created by gefei on 2016/11/15.
  */
object TestMain {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SpatialOperationExample").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val simbaContext = new SimbaContext(sc)

    import simbaContext.implicits._

    var leftData = ListBuffer[PointData]()
    var rightData = ListBuffer[PointData]()

    for (i <- 1 to 1000){
      leftData += PointData( i + 0.0, i + 0.0, i + 0.0, "a = " + i)
      rightData += PointData(i + 0.0, i + 0.0, i + 0.0, "a = " + (i + 1))
    }

    val leftRDD = sc.parallelize(leftData)
    val rightRDD = sc.parallelize(rightData)

//    leftRDD.toDF().registerTempTable("point1")
//    rightRDD.toDF().registerTempTable("point2")

    leftRDD.collect().foreach(println)

    val df = leftRDD.toDF()

    println(df)

    sc.stop()
  }
}
