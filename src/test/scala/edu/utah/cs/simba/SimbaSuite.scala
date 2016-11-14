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

package edu.utah.cs.simba

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

/**
  * Created by gefei on 2016/11/14.
  */
class SimbaSuite extends FunSuite{
  case class PointData(x: Double, y: Double, z: Double, info: String) extends Serializable
  val sparkConf = new SparkConf().setAppName("SpatialTest").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
//  val simbaContext = new SimbaContext(sc)
  import sqlContext.implicits._
  var leftData = ListBuffer[PointData]()
  var rightData = ListBuffer[PointData]()

  for (i <- 1 to 1000){
    leftData += PointData( i + 0.0, i + 0.0, i + 0.0, "a = " + i)
    rightData += PointData(i + 0.0, i + 0.0, i + 0.0, "a = " + (i + 1))
  }

  val leftRDD = sc.parallelize(leftData)
  val rightRDD = sc.parallelize(rightData)

  test("RDD Test"){
    assert(leftRDD.count() == 1000)
  }

//  leftRDD.toDF().registerTempTable("point1")
//  rightRDD.toDF().registerTempTable("point2")
//
//  test("Simple, rdd operation"){
//    val table1 = simbaContext.sql("SELECT * FROM point1")
//    table1.collect().foreach(println)
//    assert(table1.count() == 1000)
//  }
}
