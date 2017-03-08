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

import org.apache.spark.sql.simba.ShapeType
import org.apache.spark.sql.types.SQLUserDefinedType

/**
 * Created by dong on 1/15/16.
 * Multi-Dimensional Minimum Bounding Box
 */
@SQLUserDefinedType(udt = classOf[ShapeType])
case class MBR(low: Point, high: Point) extends Shape {
  require(low.dimensions == high.dimensions)
  require(low <= high)

  override val dimensions: Int = low.dimensions

  override def intersects(other: Shape): Boolean = {
    other match {
      case p: Point => contains(p)
      case mbr: MBR => intersects(mbr)
      case cir: Circle => cir.intersects(this)
      case poly: Polygon => poly.intersects(this)
      case seg: LineSegment => seg.intersects(this)
    }
  }

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
      case mbr: MBR => minDist(mbr)
      case cir: Circle => cir.minDist(this)
      case poly: Polygon => poly.minDist(this)
      case seg: LineSegment => seg.minDist(this)
    }
  }

  def this(low_x: Double, low_y: Double, high_x: Double, high_y: Double) {
    this(Point(Array(low_x, low_y)), Point(Array(high_x, high_y)))
  }

  val centroid = new Point(low.coord.zip(high.coord).map(x => (x._1 + x._2) / 2.0))

  def intersects(other: MBR): Boolean = {
    require(low.coord.length == other.low.coord.length)
    for (i <- low.coord.indices)
      if (low.coord(i) > other.high.coord(i) || high.coord(i) < other.low.coord(i)) {
        return false
      }
    true
  }

  def contains(p: Point): Boolean = {
    require(low.coord.length == p.coord.length)
    for (i <- p.coord.indices)
      if (low.coord(i) > p.coord(i) || high.coord(i) < p.coord(i)) {
        return false
      }
    true
  }

  def minDist(p: Point): Double = {
    require(low.coord.length == p.coord.length)
    var ans = 0.0
    for (i <- p.coord.indices) {
      if (p.coord(i) < low.coord(i)) {
        ans += (low.coord(i) - p.coord(i)) * (low.coord(i) - p.coord(i))
      } else if (p.coord(i) > high.coord(i)) {
        ans += (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i))
      }
    }
    Math.sqrt(ans)
  }

  def maxDist(p: Point): Double = {
    require(low.coord.length == p.coord.length)
    var ans = 0.0
    for (i <- p.coord.indices) {
      ans += Math.max((p.coord(i) - low.coord(i)) * (p.coord(i) - low.coord(i)),
        (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i)))
    }
    Math.sqrt(ans)
  }

  def minDist(other: MBR): Double = {
    require(low.coord.length == other.low.coord.length)
    var ans = 0.0
    for (i <- low.coord.indices) {
      var x = 0.0
      if (other.high.coord(i) < low.coord(i)) {
        x = Math.abs(other.high.coord(i) - low.coord(i))
      } else if (high.coord(i) < other.low.coord(i)) {
        x = Math.abs(other.low.coord(i) - high.coord(i))
      }
      ans += x * x
    }
    Math.sqrt(ans)
  }

  def area: Double = low.coord.zip(high.coord).map(x => x._2 - x._1).product

  def calcRatio(query: MBR): Double = {
    val intersect_low = low.coord.zip(query.low.coord).map(x => Math.max(x._1, x._2))
    val intersect_high = high.coord.zip(query.high.coord).map(x => Math.min(x._1, x._2))
    val diff_intersect = intersect_low.zip(intersect_high).map(x => x._2 - x._1)
    if (diff_intersect.forall(_ > 0)) 1.0 * diff_intersect.product / area
    else 0.0
  }

  override def toString: String = "MBR(" + low.toString + "," + high.toString + ")"

  def getMBR: MBR = this.copy()
}