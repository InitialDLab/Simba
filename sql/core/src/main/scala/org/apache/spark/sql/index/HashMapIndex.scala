package org.apache.spark.sql.index

import org.apache.spark.sql.Row

/**
 * Created by Dong Xie on 9/23/15.
 * Encapsulated HashMap Index
 */
class HashMapIndex[T] extends Index {
  var index = new java.util.HashMap[T, Int]()
}

object HashMapIndex {
  def apply[T](data: Array[(T, Row)]) = {
    val res = new HashMapIndex[T]
    for (i <- data.indices) {
      res.index.put(data(i)._1, i)
    }
    res
  }
}
