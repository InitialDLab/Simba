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

/**
 * Created by Dong Xie on 5/16/2015.
 * Multi Dimension Point
 */
final case class Point(coord:Array[Double], dimensions: Int)
        extends Serializable {
  def this(coord : Array[Double]) = {
    this(coord, coord.length)
  }

  def this() = {
    this(Array(), 0)
  }

  def minDist(other: Point): Double = {
    assert(dimensions == other.dimensions)
    var ans = 0.0
    for (i <- 0 to dimensions - 1)
      ans += (coord(i) - other.coord(i)) * (coord(i) - other.coord(i))
    Math.sqrt(ans)
  }

  def minDist(other: MBR): Double = other.minDist(this)

  def equals(other : Point) : Boolean = other match {
    case h : Point =>
      if (h.dimensions != dimensions) false
      else {
        var check = true
        for (i <- 0 to dimensions - 1)
          if (h.coord(i) != coord(i)) check = false
        check
      }
    case _ => false
  }

  override def toString: String = {
    var s = "("
    for (i <- 0 to dimensions - 2)
      s += coord(i) + ", "
    s += coord(dimensions - 1)
    s + ")"
  }

//  override def hashCode() : Long = {
//    var ans : Double = 0
//    for (i <- 0 to dimensions - 1)
//      ans += coord(i)
//    ans *= dimensions * 1000
//    ans.round
//  }
}
