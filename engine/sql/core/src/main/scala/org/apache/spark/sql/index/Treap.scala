/*
 *  Copyright 2016 by Simba Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.sql.index

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.spatial.Shape

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.Breaks

/**
  * Created by Dong Xie on 6/6/16.
  * Randomized BST a.k.a Treap
  */
case class TreapNode[K: Ordering: ClassTag](key: K, var data: Array[Int],
                        var left: TreapNode[K], var right: TreapNode[K],
                        rand: Long, var size: Int, var count: Int) {
  var interval: Option[(K, K)] = None

  def update(): Unit = {
    val left_size = if (left != null) left.size else 0
    val right_size = if (right != null) right.size else 0
    size = left_size + right_size + count
  }
}

case class Treap[K: Ordering: ClassTag](var root: TreapNode[K]) extends Index with Serializable {
  private val ordering = implicitly[Ordering[K]]

  def this() = this(null)

  private def calcInterval(p: TreapNode[K]): Unit = {
    if (p == null) return

    var min: K = {
      if (p.left != null) {
        calcInterval(p.left)
        p.left.interval.get._1
      } else p.key
    }

    var max: K = {
      if (p.right != null) {
        calcInterval(p.right)
        p.right.interval.get._2
      } else p.key
    }

    p.interval = Some((min, max))
  }

  private[index] def calcInterval(): Unit = calcInterval(root)

  private def leftRotate(p: TreapNode[K]): TreapNode[K] = {
    val t = p.left
    p.left = t.right
    t.right = p
    p.update()
    t.update()
    t
  }

  private def rightRotate(p: TreapNode[K]): TreapNode[K] = {
    val t = p.right
    p.right = t.left
    t.left = p
    p.update()
    t.update()
    t
  }

  private def insert(p: TreapNode[K], key: K, data: Int): TreapNode[K] = {
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

  def insert(key: K, data: Int): Unit = {
    root = insert(root, key, data)
  }

  private def rank(p: TreapNode[K], key: K): Int = {
    if (p == null) 0
    else if (ordering.lt(key, p.key)) rank(p.left, key)
    else {
      val tmp_size = if (p.left == null) 0 else p.left.size
      tmp_size + p.count + rank(p.right, key)
    }
  }

  def rank(key: K): Int = rank(root, key)

  private def getCount(p: TreapNode[K], key: K): Int = {
    if (p == null) -1
    else if (ordering.equiv(key, p.key)) p.count
    else if (ordering.lt(key, p.key)) getCount(p.left, key)
    else getCount(p.right, key)
  }

  def getCount(key: K): Int = getCount(root, key)

  private def find(p: TreapNode[K], key: K): Array[Int] = {
    if (p == null) Array()
    else if (ordering.equiv(key, p.key)) p.data
    else if (ordering.lt(key, p.key)) find(p.left, key)
    else find(p.right, key)
  }

  def find(key: K): Array[Int] = find(root, key)

  private def range(p: TreapNode[K], low: K, high: K): Array[Int] = {
    if (p == null) Array()
    else {
      var ans = mutable.ArrayBuffer[Int]()
      if (ordering.lt(low, p.key)) ans ++= range(p.left, low, high)
      if (ordering.lteq(low, p.key) && ordering.lteq(p.key, high)) ans ++= p.data
      if (ordering.lt(p.key, high)) ans ++= range(p.right, low, high)
      ans.toArray
    }
  }

  def range(low: K, high: K): Array[Int] = {
    assert(ordering.lteq(low, high))
    range(root, low, high)
  }

  private def getNumericValue(x: K): Double = x.asInstanceOf[Number].doubleValue()

  private def intersects(node: TreapNode[K], low: K, high: K): Boolean = {
    return !(ordering.gt(node.interval.get._1, high) || ordering.lt(node.interval.get._2, low))
  }

  def range(low: K, high: K, level_limit: Int, s_threshold: Double, isNumeric: Boolean)
  : Option[Array[Int]] = {
    val ans = mutable.ArrayBuffer[Int]()
    val q = new mutable.Queue[(TreapNode[K], Int)]()
    if (intersects(root, low, high)) q.enqueue((root, 1))
    var estimate: Double = 0
    var flag: Boolean = false

    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (q.nonEmpty) {
        val now = q.dequeue
        val cur_node = now._1
        val cur_level = now._2
        if (cur_level > level_limit) {
          flag = true
          break()
        }

        if (intersects(cur_node, low, high)) {
          if (cur_level == level_limit || (cur_node.left == null && cur_node.right == null)) {
            if (isNumeric) {
              val node_interval = cur_node.interval.get
              val node_low = getNumericValue(node_interval._1)
              val node_high = getNumericValue(node_interval._2)
              val intersect_low = Math.max(node_low, getNumericValue(low))
              val intersect_high = Math.min(node_high, getNumericValue(high))
              estimate += cur_node.size * (intersect_high - intersect_low) / (node_high - node_low)
            } else estimate += cur_node.size
          }

          if (ordering.lteq(low, cur_node.key) && ordering.lteq(cur_node.key, high)) {
            ans ++= cur_node.data
          }
          if (cur_node.left != null) q.enqueue((cur_node.left, cur_level + 1))
          if (cur_node.right != null) q.enqueue((cur_node.right, cur_level + 1))
        }
      }
    }

    if (!flag) return Some(ans.toArray)
    else if (estimate / root.size > s_threshold) return None

    while (q.nonEmpty) {
      val now = q.dequeue
      val cur_node = now._1
      val cur_level = now._2
      if (ordering.lteq(low, cur_node.key) && ordering.lteq(cur_node.key, high)) {
        ans ++= cur_node.data
      }
      if (cur_node.left != null) q.enqueue((cur_node.left, cur_level + 1))
      if (cur_node.right != null) q.enqueue((cur_node.right, cur_level + 1))
    }

    Some(ans.toArray)
  }
}

object Treap {
  def apply[K : Ordering: ClassTag](data: Array[(K, InternalRow)]): Treap[K] = {
    val res = new Treap[K]()
    for (i <- data.indices)
      res.insert(data(i)._1, i)
    res.calcInterval()
    res
  }
}
