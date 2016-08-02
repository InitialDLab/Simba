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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.spatial._

/**
  * Created by dong on 1/20/16.
  */
object ShapeTypeTest {
//  case class Point(name: String, x: Double, y: Double)

  case class ShapeRecord(id: Int, shape: Geometry)
  case class Record(id: Int, shape: Shape)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDRangeQuery").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", 4.toString)

    import sqlContext.implicits._
    val schema = new StructType(Array(new StructField("id", IntegerType, nullable = false),
      new StructField("shape", ShapeType, nullable = true)))

    // Polygon test
    val shapefilePath = "/path/to/shapefile.shp"
    val gf = new GeometryFactory()
    val rdd = sc.parallelize(shapefile.Parser(shapefilePath)(gf)).map(r => (r.id, r.g))
    val df = rdd.toDF().show(5)    // toDF

    val df2 = sqlContext.createDataFrame(rdd.map(org.apache.spark.sql.Row.fromTuple),
      schema).show(5)
    rdd.toDF().registerTempTable("polygons")
    sqlContext.sql("SELECT * FROM polygons").show(5) // sql test


    val rdd2 = sc.parallelize(1 to 10 map {i =>
      Record(i, LineSegment(Point(Array(i, i)), Point(Array(i, i - 1))))
    })
    rdd2.toDF().show(5) // implicit convert
    sqlContext.createDataFrame(rdd2.map(org.apache.spark.sql.Row.fromTuple),
      schema).show(5) // explicit covert
    rdd2.toDF().registerTempTable("segs")
    sqlContext.sql("SELECT * FROM segs").show(5) // sql

    println("Finished.")
  }
}

