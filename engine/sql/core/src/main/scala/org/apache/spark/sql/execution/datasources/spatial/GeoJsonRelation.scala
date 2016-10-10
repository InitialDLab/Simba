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
import play.api.libs.json._

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by gefei on 16-8-1.
  */
class GeoJsonRelation(path: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with Serializable{
  override val schema = {
    StructType(StructField("shape", ShapeType, true) :: Nil)
  }

  case class GeoJSon(`type`: String, features: List[Feature])
  case class Feature(`type`: String, properties: Option[Map[String, String]], geometry: Geometry)
  case class Geometry(`type`: String, coordinates: JsValue) {
    private def extractPoints(p: List[JsValue]) = {
      p.map {
        case (JsArray(ListBuffer(JsNumber(x), JsNumber(y)))) =>
          Point(Array(x.toDouble, y.toDouble))
      }
    }

    val shape = {
      `type` match {
        case "Point" => {
          val JsArray(ListBuffer(JsNumber(x), JsNumber(y))) = coordinates
          Point(Array(x.toDouble, y.toDouble))
        }
        case "Polygon" => {
          val JsArray(p) = coordinates.asInstanceOf[JsArray]
          val rings = p.map { case JsArray(q) => extractPoints(q.toList)}
          Polygon(rings.flatten.toArray)
        }
        case _ =>
          Point(Array(0.0, 0.0))
      }
    }
  }
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val shapes = parseShapeWithMeta(path)
    sqlContext.sparkContext.parallelize(shapes).map(Row(_))
  }

  private def parseShapeWithMeta(path: String) = {
    val json = Source.fromFile(path).mkString
    implicit val geometryFormat = Json.format[Geometry]
    implicit val featureFormat = Json.format[Feature]
    implicit val modelFormat = Json.format[GeoJSon]
    val parsed = Json.parse(json)
    val result = Json.fromJson[GeoJSon](parsed).getOrElse(null)
    if (result == null) throw new Exception("geojson input file format error.")
    result.features.map(_.geometry.shape)
  }

}
