package org.apache.spark.sql.index

import org.apache.spark.sql.Row

/**
 * Created by dong on 6/1/15.
 */

class TreeMapIndex[T] extends Index {
  var index = new java.util.TreeMap[T, Int]()
}

object TreeMapIndex {
  def apply[T](data: Array[(T, Row)]) = {
    val ans = new TreeMapIndex[T]
    for (i <- data.indices) {
      ans.index.put(data(i)._1, i)
    }
    ans
  }
}