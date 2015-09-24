package org.apache.spark.sql.index

import org.apache.spark.sql.Row

/**
 * Created by Dong Xie on 9/22/15.
 * Encapsulated TreeMap Index
 */
class TreeMapIndex[T] extends Index {
  var index = new java.util.TreeMap[T, Int]()
}

object TreeMapIndex {
  def apply[T](data: Array[(T, Row)]) = {
    val res = new TreeMapIndex[T]
    for (i <- data.indices)
      res.index.put(data(i)._1, i)
    res
  }
}