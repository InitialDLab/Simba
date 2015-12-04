package org.apache.spark.sql.index

import org.apache.spark.sql.catalyst.InternalRow

/**
 * Created by Dong Xie on 9/22/15.
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