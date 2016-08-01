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

package org.apache.spark.sql.execution.datasources.spatial

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.spatial.{Point, Polygon}
import org.apache.spark.sql.types.{ShapeType, StructField, StructType}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by gefei on 16-8-1.
  */
class GeoJsonRelation(path: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with Serializable{
  override val schema = {
    StructType(StructField("shape", ShapeType, true) :: Nil)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    sqlContext.sparkContext.parallelize(parseShapeWithMeta()).map(Row.fromTuple)
  }

  private def parseShapeWithMeta() = {
    val tree = parse(path)
    implicit val formats = org.json4s.DefaultFormats
    val result = tree.extract[GeoJSON]
    result.features.map(_.geometry.shape)
  }

  private case class Geometry(`type`: String, coordinates: JValue){
    def extractPoints(p: List[JValue]): List[Point] = {p.map{
      case JArray(List(JDouble(x), JDouble(y))) => Point(Array(x, y))}
    }
    val shape = {
      `type` match {
        case "Point" => {
          val JArray(List(JDouble(x), JDouble(y))) = coordinates
          Point(Array(x, y))
        }
//        case "Polygon" => {
//          val JArray(p) = coordinates.asInstanceOf[JArray]
//          val rings = p.map { case JArray(q) => extractPoints(q)}
//          Polygon(rings.flatten.toArray)
//        }
        case _ => Point(Array(0.0, 0.0))
      }
    }
  }
  private case class Feature(`type`: String,
                             properties: Option[Map[String, String]],
                             geometry: Geometry)

  private case class CRS(`type`: String, properties: Option[Map[String, String]])

  private case class GeoJSON(`type`: String, crs: Option[CRS], features: List[Feature])
}
