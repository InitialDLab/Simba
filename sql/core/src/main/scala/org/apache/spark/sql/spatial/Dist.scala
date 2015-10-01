package org.apache.spark.sql.spatial

/**
 * Created by Dong Xie on 9/30/15.
 * Distance Utilities
 */
object Dist {
  def furthest(a: Point, b: MBR) : Double = {
    require(a.coord.length == b.low.coord.length)
    var ans = 0.0
    for (i <- a.coord.indices) {
      ans += Math.max((a.coord(i) - b.low.coord(i)) * (a.coord(i) - b.low.coord(i)),
        (a.coord(i) - b.high.coord(i)) * (a.coord(i) - b.high.coord(i)))
    }
    Math.sqrt(ans)
  }
}
