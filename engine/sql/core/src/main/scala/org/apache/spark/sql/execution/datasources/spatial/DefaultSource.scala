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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * Created by gefei on 16-7-31.
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider{
  override def createRelation(sqlContext: SQLContext,
      parameter: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameter, null)
  }
  override def createRelation(sqlContext: SQLContext,
      parameter: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameter.getOrElse("path", sys.error(" path must be specified for shapefiles"))
    val t = parameter.getOrElse("type", "shapefile")
    t match {
      case "shapefile" => new ShapefileRelation(path)(sqlContext)
      case "geojson" => new GeoJsonRelation(path)(sqlContext)
      case "osm" => new OSMRelation(path)(sqlContext)
      case _ => ???
    }
  }
}
