package org.apache.spark.sql.spatial

/**
 * Created by Dong Xie on 9/22/15.
 * Multi-Dimensional Minimum Bounding Rectangle
 */
case class MBR(low: Point, high: Point) {
  require(low.coord.length == high.coord.length)
  require(low <= high)

  def isIntersect(other: MBR): Boolean = {
    require(low.coord.length == other.low.coord.length)
    for (i <- low.coord.indices)
      if (low.coord(i) > other.high.coord(i) || high.coord(i) < other.low.coord(i))
        return false
    true
  }

  def contains(p: Point): Boolean = {
    require(low.coord.length == p.coord.length)
    for (i <- p.coord.indices)
      if (low.coord(i) > p.coord(i) || high.coord(i) < p.coord(i))
        return false
    true
  }

  def minDist(p: Point): Double = {
    require(low.coord.length == p.coord.length)
    var ans = 0.0
    for (i <- p.coord.indices) {
      if (p.coord(i) < low.coord(i))
        ans += (low.coord(i) - p.coord(i)) * (low.coord(i) - p.coord(i))
      else if (p.coord(i) > high.coord(i))
        ans += (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i))
    }
    Math.sqrt(ans)
  }

  def minDist(other: MBR): Double = {
    require(low.coord.length == other.low.coord.length)
    var ans = 0.0
    for (i <- low.coord.indices) {
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
