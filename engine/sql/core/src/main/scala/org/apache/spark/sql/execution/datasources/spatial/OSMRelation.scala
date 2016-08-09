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
import org.apache.spark.sql.types._
import org.apache.spark.sql.spatial.Point

import scala.xml._

/**
  * Created by gefei on 16-8-1.
  */
case class OSMNode(id: Int, timestamp: String,
                   user: String, version: String, pos: Point)
case class OSMWay(nodeRefs: Seq[Int])

class OSMRelation(path: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan{
  override val schema = {
    StructType(StructField("position", ShapeType, true) :: Nil)
  }

  def nodeConverter(node: scala.xml.Node): OSMNode = {
    val id = Integer.parseInt((node \ "@id").toString().trim)
    val timestamp = (node \ "@timestamp").toString()
    val user = (node \ "@user").toString()
    val version = (node \ "@version").toString()
    val lat = java.lang.Double.parseDouble((node \ "@lat").toString())
    val lon = java.lang.Double.parseDouble((node \ "@lon").toString())
    new OSMNode(id, timestamp, user, version, Point(Array(lat, lon)))
  }

  // TODO osm extractor to be implemented here
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val xml = XML.loadFile(path)
    val tmp_nodes = xml \ "node"
    val nodes = tmp_nodes.map(nodeConverter).map(node => node.pos)
    val temp = sqlContext.sparkContext.parallelize(nodes)
    temp.map(Row(_))
  }
}
