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

/**
 * Created by dong on 1/15/16.
 * Utilities for Z-Value Curves
 */
object ZValue {
  def paddingBinaryBits(source: Int, digits: Int): String = {
    val pd_length = digits - source.toBinaryString.length
    "0" * pd_length + source.toBinaryString
  }

  // TODO shift Long to BitInt for supporting bigger Z-Values
  def apply(point: Array[Int]): Long = {
    var maxBit = 0
    for (i <- point.indices)
      if (point(i).toBinaryString.length > maxBit) {
        maxBit = point(i).toBinaryString.length
      }

    var ans = ""
    val pointStrs = point.map(x => paddingBinaryBits(x, maxBit))

    for (i <- 0 until maxBit)
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
      for (i <- 0 until dimension)
        if (currentBit - dimension + 1 + i >= 0) {
          ans(i) += shiftBase * binaryZValue(currentBit - dimension + 1 + i).toString.toInt
        }

      currentBit -= dimension
      shiftBase *= 2
    }
    Some(ans)
  }
}
