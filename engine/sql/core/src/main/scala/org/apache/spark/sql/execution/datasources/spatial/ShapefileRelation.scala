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

import com.vividsolutions.jts.geom.{GeometryFactory, Point, Polygon}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{ShapeType, StructField, StructType}

/**
  * Created by gefei on 16-7-31.
  */
class ShapefileRelation(path: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  override val schema = {
    StructType(StructField("shape", ShapeType, true) :: Nil)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val indices = requiredColumns.map(attr => schema.fieldIndex(attr))
    val gf = new GeometryFactory()
    val shapes = ShapeFile.Parser(path)(gf)
    val shapefileRdd = sqlContext.sparkContext
      .parallelize(shapes).map(_.g match {
      case p: Polygon => org.apache.spark.sql.spatial.Polygon(p)
      case p: Point => org.apache.spark.sql.spatial.Point(p)
//      case _ => ???
    })

    shapefileRdd map Row.fromTuple
  }
}
