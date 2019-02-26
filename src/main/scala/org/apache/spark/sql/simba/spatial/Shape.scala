/*
 * Copyright 2017 by Simba Project
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
 *
 */

package org.apache.spark.sql.simba.spatial

import com.vividsolutions.jts.geom.{Geometry, Polygon => JTSPolygon}
import org.apache.spark.sql.simba.ShapeType
import org.apache.spark.sql.types.SQLUserDefinedType

/**
  * Created by dong on 3/16/16.
  */
@SQLUserDefinedType(udt = classOf[ShapeType])
abstract class Shape extends Serializable {
  def minDist(other: Shape): Double

  def intersects(other: Shape): Boolean

  def getMBR: MBR

  val dimensions: Int
}

object Shape {
  final def apply(g: Geometry): Shape = g match {
    case jtsPolygon : JTSPolygon => new Polygon(jtsPolygon)
    case _ => null
  }
}
