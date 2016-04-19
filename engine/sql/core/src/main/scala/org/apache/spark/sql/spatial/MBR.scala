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
 * MBR for Multi Dimension Space
 */
case class MBR(low: Point, high: Point) {
  require (low.dimensions == high.dimensions)

  val centroid = new Point((0 to low.dimensions - 1).map(i => (low.coord(i) + high.coord(i)) / 2.0).toArray)

  def isIntersect(other : MBR) : Boolean = {
    assert(low.dimensions == other.low.dimensions)
    for (i <- 0 to low.dimensions - 1)
      if (low.coord(i) > other.high.coord(i) || high.coord(i) < other.low.coord(i))
        return false
    true
  }

  def contains(p : Point) : Boolean = {
    assert(low.dimensions == p.dimensions)
    for (i <- 0 to p.dimensions - 1)
      if (low.coord(i) > p.coord(i) || high.coord(i) < p.coord(i))
        return false
    true
  }

  def minDist(p : Point) : Double = {
    assert(low.dimensions == p.dimensions)
    var ans = 0.0
    for (i <- 0 to p.dimensions - 1) {
      if (p.coord(i) < low.coord(i))
        ans += (low.coord(i) - p.coord(i)) * (low.coord(i) - p.coord(i))
      else if (p.coord(i) > high.coord(i))
        ans += (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i))
    }
    Math.sqrt(ans)
  }

  def minDist(other: MBR) : Double = {
    assert(other.low.dimensions == low.dimensions)
    var ans = 0.0
    for (i <- 0 to low.dimensions - 1) {
      var x = 0.0
      if (other.high.coord(i) < low.coord(i))
        x = Math.abs(other.high.coord(i) - low.coord(i))
      else if (high.coord(i) < other.low.coord(i))
        x = Math.abs(other.low.coord(i) - high.coord(i))
      ans += x * x
    }
    Math.sqrt(ans)
  }
}
