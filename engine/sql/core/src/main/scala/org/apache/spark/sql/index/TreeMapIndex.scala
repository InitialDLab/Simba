package org.apache.spark.sql.index

import org.apache.spark.sql.catalyst.InternalRow

/**
 * Created by dong on 1/15/16.
 * Encapsulated TreeMap Index
 */
class TreeMapIndex[T] extends Index with Serializable {
  var index = new java.util.TreeMap[T, Int]()
}

object TreeMapIndex {
  def apply[T](data: Array[(T, InternalRow)]) = {
    val res = new TreeMapIndex[T]
    for (i <- data.indices)
      res.index.put(data(i)._1, i)
    res
  }
}