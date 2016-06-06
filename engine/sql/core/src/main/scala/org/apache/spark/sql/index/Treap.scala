package org.apache.spark.sql.index

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by Dong Xie on 6/6/16.
  * Randomized BST a.k.a Treap
  */
case class TreapNode[K](key: K, var data: Array[Int],
                        var left: TreapNode[K], var right: TreapNode[K],
                        rand: Long, var size: Int, var count: Int) {
  def update(): Unit = {
    val left_size = if (left != null) left.size else 0
    val right_size = if (left != null) left.size else 0
    size = left_size + right_size + 1
  }
}

case class Treap[K: Ordering: ClassTag](var root: TreapNode[K]) extends Index with Serializable {
  private val ordering = implicitly[Ordering[K]]

  def leftRotate(p: TreapNode[K]): TreapNode[K] = {
    val t = p.left
    p.left = t.right
    t.right = p
    p.update()
    t.update()
    t
  }

  def rightRotate(p: TreapNode[K]): TreapNode[K] = {
    val t = p.right
    p.right = t.left
    t.left = p
    p.update()
    t.update()
    t
  }

  def insert(p: TreapNode[K], key: K, data: Int): TreapNode[K] = {
    if (p == null) {
      new TreapNode(key, Array(data), null, null, Random.nextLong(), 1, 1)
    } else if (ordering.equiv(key, p.key)) {
      p.count += 1
      p.size += 1
      p.data = p.data :+ data
      p
    } else if (ordering.lt(key, p.key)) {
      p.left = insert(p.left, key, data)
      if (p.left.rand < p.rand) leftRotate(p)
      else {
        p.update()
        p
      }
    } else {
      p.right = insert(p.right, key, data)
      if (p.right.rand < p.rand) rightRotate(p)
      else {
        p.update()
        p
      }
    }
  }

  def rank(p: TreapNode[K], key: K): Int = {
    if (p == null) 0
    else if (ordering.lt(key, p.key)) rank(p.left, key)
    else p.left.size + p.count + rank(p.right, key)
  }

  def getCount(p: TreapNode[K], key: K): Int = {
    if (p == null) -1
    else if (ordering.equiv(key, p.key)) p.count
    else if (ordering.lt(key, p.key)) getCount(p.left, key)
    else getCount(p.right, key)
  }

  def find(p: TreapNode[K], key: K): Array[Int] = {
    if (p == null) Array()
    else if (ordering.equiv(key, p.key)) p.data
    else if (ordering.lt(key, p.key)) find(p.left, key)
    else find(p.right, key)
  }

  private def range(p: TreapNode[K], low: K, high: K): Array[Int] = {
    if (p == null) Array()
    else {
      var ans = mutable.ArrayBuffer[Int]()
      if (ordering.lt(low, p.key)) ans ++= range(p.left, low, p.key)
      if (ordering.lteq(low, p.key) && ordering.lteq(p.key, high)) ans ++= p.data
      if (ordering.lt(p.key, high)) ans ++= range(p.right, p.key, high)
      ans.toArray
    }
  }

  def range(low: K, high: K): Array[Int] = {
    assert(ordering.lteq(low, high))
    range(root, low, high)
  }
}

object Treap {
  def apply[K : Ordering: ClassTag](data: Array[(K, InternalRow)]): Treap[K] = {
    val res = new Treap[K](null)
    for (i <- data.indices)
      res.insert(res.root, data(i)._1, i)
    res
  }
}
