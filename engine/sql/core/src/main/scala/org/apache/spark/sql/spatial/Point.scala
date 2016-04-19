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
 * Created by Dong Xie on 9/22/15.
 * Multi-Dimensional Point
 */
final case class Point(coord: Array[Double]) extends Serializable {
  def this() = this(Array())

  def minDist(other: Point): Double = {
    require(coord.length == other.coord.length)
    var ans =0.0
    for (i <- coord.indices)
      ans += (coord(i) - other.coord(i)) * (coord(i) - other.coord(i))
    Math.sqrt(ans)
  }

  def minDist(other: MBR): Double = other.minDist(this)

  def equals(other: Point): Boolean = other match {
    case p: Point =>
      if (p.coord.length != coord.length) false
      else {
        for (i <- coord.indices)
          if (coord(i) != p.coord(i)) return false
        true
      }
    case _ => false
  }

  def <=(other: Point): Boolean = {
    for (i <- coord.indices)
      if (coord(i) > other.coord(i)) return false
    true
  }

  def toMBR = new MBR(this, this)

  override def toString: String = {
    var s = "POINT("
    s += coord(0).toString
    for (i <- 1 to coord.length - 1) s += "," + coord(i)
    s + ")"
  }
}
