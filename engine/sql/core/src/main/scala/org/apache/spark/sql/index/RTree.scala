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

import org.apache.spark.sql.spatial._

import scala.collection.mutable
import scala.util.control.Breaks

/**
 * Created by dong on 1/15/16.
 * Static Multi-Dimensional R-Tree Index
 */
abstract class RTreeEntry {
  def minDist(x: Shape): Double

  def intersects(x: Shape): Boolean
}

case class RTreeLeafEntry(shape: Shape, m_data: Int, size: Int) extends RTreeEntry {
  override def minDist(x: Shape): Double = shape.minDist(x)
  override def intersects(x: Shape): Boolean = x.intersects(shape)
}

case class RTreeInternalEntry(mbr: MBR, node: RTreeNode) extends RTreeEntry {
  override def minDist(x: Shape): Double = mbr.minDist(x)
  override def intersects(x: Shape): Boolean = x.intersects(mbr)
}

case class RTreeNode(m_mbr: MBR, m_child: Array[RTreeEntry], isLeaf: Boolean) {
  def this(m_mbr: MBR, children: Array[(MBR, RTreeNode)]) = {
    this(m_mbr, children.map(x => new RTreeInternalEntry(x._1, x._2)), false)
  }

  // XX Interesting Trick! Overriding same function
  def this(m_mbr: MBR, children: => Array[(Point, Int)]) = {
    this(m_mbr, children.map(x => new RTreeLeafEntry(x._1, x._2, 1)), true)
  }

  def this(m_mbr: MBR, children: Array[(MBR, Int, Int)]) = {
    this(m_mbr, children.map(x => new RTreeLeafEntry(x._1, x._2, x._3)), true)
  }

  val size: Long = {
    if (isLeaf) m_child.map(x => x.asInstanceOf[RTreeLeafEntry].size).sum
    else m_child.map(x => x.asInstanceOf[RTreeInternalEntry].node.size).sum
  }
}

class NNOrdering() extends Ordering[(_, Double)] {
  def compare(a: (_, Double), b: (_, Double)): Int = -a._2.compare(b._2)
}

case class RTree(root: RTreeNode) extends Index with Serializable {
  def range(query: MBR): Array[(Shape, Int)] = {
    val ans = mutable.ArrayBuffer[(Shape, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.intersects(query) && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach {
          case RTreeInternalEntry(mbr, node) =>
            if (query.intersects(mbr)) st.push(node)
        }
      } else {
        now.m_child.foreach {
          case RTreeLeafEntry(shape, m_data, _) =>
            if (query.intersects(shape)) ans += ((shape, m_data))
        }
      }
    }
    ans.toArray
  }

  def range(query: MBR, level_limit: Int, s_threshold: Double): Option[Array[(Shape, Int)]] = {
    val ans = mutable.ArrayBuffer[(Shape, Int)]()
    val q = new mutable.Queue[(RTreeNode, Int)]()
    if (root.m_mbr.intersects(query) && root.m_child.nonEmpty) q.enqueue((root, 1))
    var estimate: Double = 0
    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (q.nonEmpty) {
        val now = q.dequeue
        val cur_node = now._1
        val cur_level = now._2
        if (cur_node.isLeaf) {
          cur_node.m_child.foreach {
            case RTreeLeafEntry(shape, m_data, _) =>
              if (query.intersects(shape)) ans += ((shape, m_data))
          }
        } else if (cur_level < level_limit) {
          cur_node.m_child.foreach {
            case RTreeInternalEntry(mbr, node) =>
              if (query.intersects(mbr)) q.enqueue((node, cur_level + 1))
          }
        } else if (cur_level == level_limit) {
          estimate += cur_node.m_mbr.calcRatio(query) * cur_node.size
          cur_node.m_child.foreach {
            case RTreeInternalEntry(mbr, node) =>
              if (query.intersects(mbr)) q.enqueue((node, cur_level + 1))
          }
        } else break
      }
    }
    if (ans.nonEmpty) return Some(ans.toArray)
    else if (estimate / root.size > s_threshold) return None
    while (q.nonEmpty) {
      val now = q.dequeue
      val cur_node = now._1
      val cur_level = now._2
      if (cur_node.isLeaf) {
        cur_node.m_child.foreach {
          case RTreeLeafEntry(shape, m_data, _) =>
            if (query.intersects(shape)) ans += ((shape, m_data))
        }
      } else {
        cur_node.m_child.foreach {
          case RTreeInternalEntry(mbr, node) =>
            if (query.intersects(mbr)) q.enqueue((node, cur_level + 1))
        }
      }
    }
    Some(ans.toArray)
  }

  def circleRange(origin: Shape, r: Double): Array[(Shape, Int)] = {
    val ans = mutable.ArrayBuffer[(Shape, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach{
          case RTreeInternalEntry(mbr, node) =>
            if (origin.minDist(mbr) <= r) st.push(node)
        }
      } else {
        now.m_child.foreach {
          case RTreeLeafEntry(shape, m_data, _) =>
            if (origin.minDist(shape) <= r) ans += ((shape, m_data))
        }
      }
    }
    ans.toArray
  }

  def circleRangeCnt(origin: Shape, r: Double): Array[(Shape, Int, Int)] = {
    val ans = mutable.ArrayBuffer[(Shape, Int, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach{
          case RTreeInternalEntry(mbr, node) =>
            if (origin.minDist(mbr) <= r) st.push(node)
        }
      } else {
        now.m_child.foreach {
          case RTreeLeafEntry(shape, m_data, size) =>
            if (origin.minDist(shape) <= r) ans += ((shape, m_data, size))
        }
      }
    }
    ans.toArray
  }

  def circleRangeConj(queries: Array[(Point, Double)]): Array[(Shape, Int)] = {
    val ans = mutable.ArrayBuffer[(Shape, Int)]()
    val st = new mutable.Stack[RTreeNode]()

    def check(now: Shape) : Boolean = {
      for (i <- queries.indices)
        if (now.minDist(queries(i)._1) > queries(i)._2) return false
      true
    }

    if (check(root.m_mbr) && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) now.m_child.foreach {
        case RTreeInternalEntry(mbr, node) =>
          if (check(mbr)) st.push(node)
      } else {
        now.m_child.foreach {
          case RTreeLeafEntry(shape, m_data, _) =>
            if (check(shape)) ans += ((shape, m_data))
        }
      }
    }
    ans.toArray
  }

  def kNN(query: Point, k: Int, keepSame: Boolean = false): Array[(Shape, Int)] = {
    val ans = mutable.ArrayBuffer[(Shape, Int)]()
    val pq = new mutable.PriorityQueue[(_, Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((root, 0.0))

    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis)) break()

        now._1 match {
          case RTreeNode(_, m_child, isLeaf) =>
            m_child.foreach(entry =>
              if (isLeaf) pq.enqueue((entry, entry.minDist(query)))
              else pq.enqueue((entry.asInstanceOf[RTreeInternalEntry].node, entry.minDist(query)))
            )
          case RTreeLeafEntry(p, m_data, size) =>
            cnt += size
            kNN_dis = now._2
            ans += ((p, m_data))
        }
      }
    }

    ans.toArray
  }

  def kNN(query: Point, distFunc: (Point, MBR) => Double,
          k: Int, keepSame: Boolean): Array[(Shape, Int)] = {
    val ans = mutable.ArrayBuffer[(Shape, Int)]()
    val pq = new mutable.PriorityQueue[(_, Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((root, 0.0))

    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis)) break()

        now._1 match {
          case RTreeNode(_, m_child, _) =>
            m_child.foreach {
              case entry @ RTreeInternalEntry(mbr, node) =>
                pq.enqueue((node, distFunc(query, mbr)))
              case entry @ RTreeLeafEntry(mbr, m_data, size) =>
                require(mbr.isInstanceOf[MBR])
                pq.enqueue((entry, distFunc(query, mbr.asInstanceOf[MBR])))
            }
          case RTreeLeafEntry(mbr, m_data, size) =>
            cnt += size
            kNN_dis = now._2
            ans += ((mbr, m_data))
        }
      }
    }

    ans.toArray
  }

  def kNN(query: MBR, distFunc: (MBR, MBR) => Double,
          k: Int, keepSame: Boolean): Array[(Shape, Int)] = {
    val ans = mutable.ArrayBuffer[(Shape, Int)]()
    val pq = new mutable.PriorityQueue[(_, Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((root, 0.0))

    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis)) break()

        now._1 match {
          case RTreeNode(_, m_child, _) =>
            m_child.foreach {
              case entry @ RTreeInternalEntry(mbr, node) =>
                pq.enqueue((node, distFunc(query, mbr)))
              case entry @ RTreeLeafEntry(mbr, m_data, size) =>
                require(mbr.isInstanceOf[MBR])
                pq.enqueue((entry, distFunc(query, mbr.asInstanceOf[MBR])))
            }
          case RTreeLeafEntry(mbr, m_data, size) =>
            cnt += size
            kNN_dis = now._2
            ans += ((mbr, m_data))
        }
      }
    }
    ans.toArray
  }
}

object RTree {
  def apply(entries: Array[(Point, Int)], max_entries_per_node: Int): RTree = {
    val dimension = entries(0)._1.coord.length
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0/(dimension - i))).toInt
      remaining /= dim(i)
    }

    def recursiveGroupPoint(entries: Array[(Point, Int)],
                            cur_dim: Int, until_dim: Int): Array[Array[(Point, Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(_._1.coord(cur_dim) < _._1.coord(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.map(now => recursiveGroupPoint(now, cur_dim + 1, until_dim)).flatMap(list => list)
      } else grouped
    }

    val grouped = recursiveGroupPoint(entries, 0, dimension - 1)
    val rtree_nodes = mutable.ArrayBuffer[(MBR, RTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(x => Double.MaxValue)
      val max = new Array[Double](dimension).map(x => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.coord(i))
        for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.coord(i))
      })
      val mbr = new MBR(new Point(min), new Point(max))
      rtree_nodes += ((mbr, new RTreeNode(mbr, list)))
    })

    var cur_rtree_nodes = rtree_nodes.toArray
    var cur_len = cur_rtree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def over(dim: Array[Int]): Boolean = {
      for (i <- dim.indices)
        if (dim(i) != 1) return false
      true
    }

    def comp(dim: Int)(left: (MBR, RTreeNode), right: (MBR, RTreeNode)): Boolean = {
      val left_center = left._1.low.coord(dim) + left._1.high.coord(dim)
      val right_center = right._1.low.coord(dim) + right._1.high.coord(dim)
      left_center < right_center
    }

    def recursiveGroupRTreeNode(entries: Array[(MBR, RTreeNode)], cur_dim: Int, until_dim: Int)
    : Array[Array[(MBR, RTreeNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.map(now => recursiveGroupRTreeNode(now, cur_dim + 1, until_dim))
          .flatMap(list => list)
      } else grouped
    }

    while (!over(dim)) {
      val grouped = recursiveGroupRTreeNode(cur_rtree_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(MBR, RTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(x => Double.MaxValue)
        val max = new Array[Double](dimension).map(x => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
          for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
        })
        val mbr = new MBR(new Point(min), new Point(max))
        tmp_nodes += ((mbr, new RTreeNode(mbr, list)))
      })
      cur_rtree_nodes = tmp_nodes.toArray
      cur_len = cur_rtree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 until dimension) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }

    val min = new Array[Double](dimension).map(x => Double.MaxValue)
    val max = new Array[Double](dimension).map(x => Double.MinValue)
    cur_rtree_nodes.foreach(now => {
      for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
      for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
    })

    val mbr = new MBR(new Point(min), new Point(max))
    val root = new RTreeNode(mbr, cur_rtree_nodes)
    new RTree(root)
  }

  def apply(entries: Array[(MBR, Int, Int)], max_entries_per_node: Int): RTree = {
    val dimension = entries(0)._1.low.coord.length
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def compMBR(dim: Int)(left: (MBR, Int, Int), right: (MBR, Int, Int)): Boolean = {
      val left_center = left._1.low.coord(dim) + left._1.high.coord(dim)
      val right_center = right._1.low.coord(dim) + right._1.high.coord(dim)
      left_center < right_center
    }

    def recursiveGroupMBR(entries: Array[(MBR, Int, Int)], cur_dim: Int, until_dim: Int)
    : Array[Array[(MBR, Int, Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(compMBR(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.map(now => recursiveGroupMBR(now, cur_dim + 1, until_dim)).flatMap(list => list)
      } else grouped
    }

    val grouped = recursiveGroupMBR(entries, 0, dimension - 1)
    val rtree_nodes = mutable.ArrayBuffer[(MBR, RTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(x => Double.MaxValue)
      val max = new Array[Double](dimension).map(x => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
        for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
      })
      val mbr = new MBR(new Point(min), new Point(max))
      rtree_nodes += ((mbr, new RTreeNode(mbr, list)))
    })

    var cur_rtree_nodes = rtree_nodes.toArray
    var cur_len = cur_rtree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def over(dim : Array[Int]) : Boolean = {
      for (i <- dim.indices)
        if (dim(i) != 1) return false
      true
    }

    def comp(dim: Int)(left : (MBR, RTreeNode), right : (MBR, RTreeNode)) : Boolean = {
      val left_center = left._1.low.coord(dim) + left._1.high.coord(dim)
      val right_center = right._1.low.coord(dim) + right._1.high.coord(dim)
      left_center < right_center
    }

    def recursiveGroupRTreeNode(entries: Array[(MBR, RTreeNode)],
                                cur_dim : Int, until_dim : Int) : Array[Array[(MBR, RTreeNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.map(now => {
          recursiveGroupRTreeNode(now, cur_dim + 1, until_dim)
        }).flatMap(list => list)
      } else grouped
    }

    while (!over(dim)) {
      val grouped = recursiveGroupRTreeNode(cur_rtree_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(MBR, RTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(x => Double.MaxValue)
        val max = new Array[Double](dimension).map(x => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
          for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
        })
        val mbr = new MBR(new Point(min), new Point(max))
        tmp_nodes += ((mbr, new RTreeNode(mbr, list)))
      })
      cur_rtree_nodes = tmp_nodes.toArray
      cur_len = cur_rtree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 until dimension) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }

    val min = new Array[Double](dimension).map(x => Double.MaxValue)
    val max = new Array[Double](dimension).map(x => Double.MinValue)
    cur_rtree_nodes.foreach(now => {
      for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
      for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
    })

    val mbr = new MBR(new Point(min), new Point(max))
    val root = new RTreeNode(mbr, cur_rtree_nodes)
    new RTree(root)
  }
}