package org.apache.spark.sql.spatial

/**
 * Created by Dong Xie on 5/2/2015.
 * An object to calculate ZValue of a point
 */

object ZValue {
  def paddingBinaryBits(source: Int, digits: Int): String = {
    val paddingLength = digits - source.toBinaryString.length
    "0" * paddingLength + source.toBinaryString
  }

  //TODO shift Long to BigInt for supporting bigger Z-Values
  def apply(point: Array[Int]) : Long = {
    var maxBit = 0
    for (i <- 0 to point.length - 1)
      if (point(i).toBinaryString.length > maxBit)
        maxBit = point(i).toBinaryString.length

    var ans = ""
    val pointStrs = point.map(x => paddingBinaryBits(x, maxBit))

    for (i <- 0 to maxBit - 1)
      for (j <- 0 to point.length - 1)
        ans += pointStrs(j)(i)

    java.lang.Long.parseLong(ans, 2)
  }

  def unapply(ops: (Long, Int)) : Option[Array[Int]] = {
    val (value, dimension) = ops
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
