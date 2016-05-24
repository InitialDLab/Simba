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

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dong on 1/20/16.
  */
object ShapeTypeTest {
//  case class Point(name: String, x: Double, y: Double)

  case class Point(name: Int, date: String, id: Int)
  case class ShapeRecord(id: Int, shape: Geometry)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDRangeQuery").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", 4.toString)

    import sqlContext.implicits._

    val shapefilePath = "/home/gefei/Downloads/gis/tl_2015_04_elsd/tl_2015_04_elsd.shp"

    val gf = new GeometryFactory()
    val rdd = sc.parallelize(shapefile.Parser(shapefilePath)(gf)).map(r => (r.id, r.g))
    val df = rdd.toDF()
    df.show(5)

    val schema = new StructType(Array(new StructField("id", IntegerType, nullable = false),
                                      new StructField("shape", ShapeType, nullable = true)))
    val transferedRdd = rdd.map(org.apache.spark.sql.Row.fromTuple)
    val df2 = sqlContext.createDataFrame(transferedRdd, schema)
    df2.show(5)

    println("Finished.")
  }
}
