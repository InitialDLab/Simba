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

package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.spatial.{Point, Polygon, Shape}
import org.apache.spark.sql.types.{IntegerType, ShapeType, StructField, StructType}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by gefei on 16-7-25.
  */
object GeoJSonFileExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GeoJsonFileExample").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val rdd = sc.textFile("/home/gefei/data/geo_data/custom.geo.json")
    val transfered = rdd.flatMap(item => parseShapeWithMeta(item).map(_._1).zipWithIndex)

    val schema = new StructType(Array(new StructField("shape", ShapeType, nullable = true),
      new StructField("id", IntegerType, nullable = false)))
    val df = sqlContext.createDataFrame(transfered.map(org.apache.spark.sql.Row.fromTuple), schema)

    df.show(5)
    sc.stop()
  }

  def parseShapeWithMeta(line: String): Seq[(Shape, Option[Map[String, String]])] = {
    val tree = parse(line)
    implicit val formats = org.json4s.DefaultFormats
    val result = tree.extract[GeoJSON]
    result.features.map(f => (f.geometry.shape, f.properties))
  }
}
private case class Geometry(`type`: String, coordinates: JValue) {
  def extractPoints(p: List[JValue]): List[Point] = {
    p.map { case (JArray(List(JDouble(x), JDouble(y)))) => Point(Array(x, y))}
  }

  val shape = {
    `type` match {
      case "Point" => {
        val JArray(List(JDouble(x), JDouble(y))) = coordinates
        Point(Array(x, y))
      }
      case "Polygon" => {
        val JArray(p) = coordinates.asInstanceOf[JArray]
        val rings = p.map { case JArray(q) => extractPoints(q)}
        Polygon(rings.flatten.toArray)
      }
//      case "MultiPolygon" => {
//        val JArray(p) = coordinates.asInstanceOf[JArray]
//        val rings = p.map{case JArray(q) => {
//          val internal_ring = q match {
//            case JArray(qq) => extractPoints(qq)
//          }
//          Polygon(internal_ring.toArray)
//        }}
//        rings
//      }
      case _ => Point(Array(0, 0))
    }
  }

  println("xxxxxxxxxx: " + `type`)

}
private case class Feature(`type`: String,
                            properties: Option[Map[String, String]],
                            geometry: Geometry)

private case class CRS(`type`: String, properties: Option[Map[String, String]])

private case class GeoJSON(`type`: String, crs: Option[CRS], features: List[Feature])