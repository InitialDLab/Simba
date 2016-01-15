package org.apache.spark.sql.index

import org.apache.spark.sql.catalyst.InternalRow

/**
 * Created by dong on 1/15/16.
 * Encapsulated HashMap Index
 */
class HashMapIndex[T] extends Index with Serializable {
  var index = new java.util.HashMap[T, Int]()
}

object HashMapIndex {
  def apply[T](data: Array[(T, InternalRow)]) = {
    val res = new HashMapIndex[T]
    for (i <- data.indices) {
      res.index.put(data(i)._1, i)
    }
    res
  }
}