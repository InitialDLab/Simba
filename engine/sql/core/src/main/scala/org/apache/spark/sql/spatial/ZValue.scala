package org.apache.spark.sql.spatial

/**
 * Created by Dong on 9/22/15.
 * Utilities for Z-Value Curves
 */
object ZValue {
  def paddingBinaryBits(source: Int, digits: Int): String = {
    val pd_length = digits - source.toBinaryString.length
    "0" * pd_length + source.toBinaryString
  }

  //TODO shift Long to BitInt for supporting bigger Z-Values
  def apply(point: Array[Int]): Long = {
    var maxBit = 0
    for (i <- point.indices)
      if (point(i).toBinaryString.length > maxBit)
        maxBit = point(i).toBinaryString.length

    var ans = ""
    val pointStrs = point.map(x => paddingBinaryBits(x, maxBit))

    for (i <- 0 to maxBit - 1)
        for (j <- point.indices)
          ans += pointStrs(j)(i)

    java.lang.Long.parseLong(ans, 2)
  }

  def unapply(value: Long, dimension: Int): Option[Array[Int]] = {
    val ans = new Array[Int](dimension)
    val binaryZValue = value.toBinaryString
    var currentBit = binaryZValue.length - 1
    var shiftBase = 1
    while (currentBit >= 0) {
      for (i <- 0 to dimension - 1)
        if (currentBit - dimension + 1 + i >= 0)
          ans(i) += shiftBase * binaryZValue(currentBit - dimension + 1 + i).toString.toInt

      currentBit -= dimension
      shiftBase *= 2
    }
    Some(ans)
  }
}
