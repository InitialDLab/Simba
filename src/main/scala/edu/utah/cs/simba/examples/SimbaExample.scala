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

package edu.utah.cs.simba.examples

import edu.utah.cs.simba.SimbaContext
import edu.utah.cs.simba.index.RTreeType
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 *  A few examples for simba project
 */
object SimbaExample {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SpatialOperationExample").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val simbaContext = new SimbaContext(sc)

    var leftData = ListBuffer[PointData]()
    var rightData = ListBuffer[PointData]()

    import simbaContext.implicits._
    import simbaContext.SimbaImplicits._

    for (i <- 1 to 1000){
      leftData += PointData( i + 0.0, i + 0.0, i + 0.0, "a = " + i)
      rightData += PointData(i + 0.0, i + 0.0, i + 0.0, "a = " + (i + 1))
    }

    val leftDF = sc.parallelize(leftData).toDF
    val rightDF = sc.parallelize(rightData).toDF

    leftDF.registerTempTable("point1")
    rightDF.registerTempTable("point2")

    // Simba RTree index create
    println("--------------------------\n Creating index \n--------------------------")
    leftDF.index(RTreeType, "rt", Array("x", "y"))

    // Simba single table query
    println("--------------------------\n Indexed knn query\n--------------------------")
    leftDF.knn(Array("x", "y"), Array(10.0, 10), 3).show()
    println("--------------------------\n Indexed range query\n--------------------------")
    leftDF.range(Array("x", "y"), Array(4.0, 5.0), Array(111.0, 222.0)).show(10)

    // Simba join operator
    println("--------------------------\n knn join operation\n--------------------------")
    leftDF.knnJoin(rightDF, Array("x", "y"), Array("x", "y"), 3).show(10)
    println("--------------------------\n distance join operation\n--------------------------")
    leftDF.distanceJoin(rightDF, Array("x", "y"), Array("x", "y"), 1.42).show(10)

    sc.stop()
  }
}
