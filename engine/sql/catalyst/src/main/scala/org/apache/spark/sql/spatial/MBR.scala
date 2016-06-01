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

package org.apache.spark.sql.spatial

import scala.collection.mutable.ListBuffer

/**
 * Created by dong on 1/15/16.
 * Multi-Dimensional Minimum Bounding Box
 */
case class MBR(low: Point, high: Point) extends Shape {
  require(low.coord.length == high.coord.length)
  require(low <= high)

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

  val centroid = new Point(low.coord.zip(high.coord).map(x => x._1 + x._2 / 2.0))

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

  def intersectRadio(other: MBR) : Double = {
    require(low.coord.length == other.low.coord.length)
    if (!intersects(other)) {
      0.0
    } else if (other.contains(low) && other.contains(high)){
      1.0
    } else {
      var interset_edges = new ListBuffer[Double]()
      for (i <- low.coord.indices) {
        val i_length = high.coord{i} - low.coord{i}
        val left_length = 0.0 max (other.low.coord{i} - low.coord{i})
        val right_length = 0.0 max (high.coord{i} - other.high.coord{i})
        interset_edges += (i_length - left_length - right_length)
      }
      val interset_area = interset_edges.fold(1.0)((a: Double, b: Double) => a * b)
      val this_area = high.coord.toList.zip(low.coord.toList).map(m => m._1 - m._2).product

      interset_area / this_area
    }
  }

  override def toString: String = "(" + low.toString + "," + high.toString + ")"

  def getMBR: MBR = this.copy()
}

