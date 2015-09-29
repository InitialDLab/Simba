package org.apache.spark.sql.index

import org.apache.spark.sql.spatial._

import scala.collection.mutable
import scala.util.control.Breaks

/**
 * Created by Dong Xie on 9/23/15.
 * Static Multi-Dimensional R-Tree Index
 */

case class RTreeEntry(region: MBR, node: RTreeNode, m_data: Int, nodeSize: Int)

case class RTreeNode(m_mbr: MBR, m_child: Array[RTreeEntry], isLeaf: Boolean) {
  def this(m_mbr: MBR, children: Array[(MBR, RTreeNode)]) = {
    this(m_mbr, children.map(x => new RTreeEntry(x._1, x._2, -1, -1)), false)
  }

  //XX Interesting Trick!
  def this(m_mbr: MBR, children: => Array[(Point, Int)]) = {
    this(m_mbr, children.map(x => new RTreeEntry(x._1.toMBR, null, x._2, 1)), true)
  }

  def this(m_mbr: MBR, children: Array[(MBR, Int, Int)]) = {
    this(m_mbr, children.map(x => new RTreeEntry(x._1, null, x._2, x._3)), true)
  }
}

class NNOrdering() extends Ordering[(_, Double)] {
  def compare(a: (_, Double), b: (_, Double)): Int = -a._2.compare(b._2)
}

case class RTree(root: RTreeNode) extends Index {
  def range(query: MBR): Array[(MBR, Int)] = {
    val ans = mutable.ArrayBuffer[(MBR, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.isIntersect(query) && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf)
        now.m_child.foreach(entry => if (query.isIntersect(entry.region)) st.push(entry.node))
      else now.m_child.foreach(entry => if (query.isIntersect(entry.region)) ans += ((entry.region, entry.m_data)))
    }
    ans.toArray
  }

  def circleRange(origin: Point, r: Double): Array[(MBR, Int)] = {
    val ans = mutable.ArrayBuffer[(MBR, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf)
        now.m_child.foreach(entry => if (origin.minDist(entry.region) <= r) st.push(entry.node))
      else now.m_child.foreach(entry => if (origin.minDist(entry.region) <= r) ans += ((entry.region, entry.m_data)))
    }
    ans.toArray
  }

  def circleRange(origin: MBR, r: Double): Array[(MBR, Int)] = {
    val ans = mutable.ArrayBuffer[(MBR, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf)
        now.m_child.foreach(entry => if (origin.minDist(entry.region) <= r) st.push(entry.node))
      else now.m_child.foreach(entry => if (origin.minDist(entry.region) <= r) ans += ((entry.region, entry.m_data)))
    }
    ans.toArray
  }

  def circleRangeConj(queries: Array[(Point, Double)]): Array[(MBR, Int)] = {
    val ans = mutable.ArrayBuffer[(MBR, Int)]()
    val st = new mutable.Stack[RTreeNode]()

    def checkMBR(mbr: MBR) : Boolean = {
      for (i <- queries.indices)
        if (mbr.minDist(queries(i)._1) > queries(i)._2)
          return false
      true
    }

    if (checkMBR(root.m_mbr) && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf)
        now.m_child.foreach(entry => if (checkMBR(entry.region)) st.push(entry.node))
      else now.m_child.foreach(entry => if (checkMBR(entry.region)) ans += ((entry.region, entry.m_data)))
    }
    ans.toArray
  }

  def kNN(query: Point, k: Int, keepSame: Boolean = false): Array[(MBR, Int)] = {
    val ans = mutable.ArrayBuffer[(MBR, Int)]()
    val pq = new mutable.PriorityQueue[(_, Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((root, 0.0))

    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis))
          break()

        now._1 match {
          case node: RTreeNode =>
            node.m_child.foreach(entry =>
              if (node.isLeaf) pq.enqueue((entry, entry.region.minDist(query)))
              else pq.enqueue((entry.node, entry.region.minDist(query)))
            )
          case entry: RTreeEntry =>
            cnt += 1
            kNN_dis = now._2
            ans += ((entry.region, entry.m_data))
        }
      }
    }

    ans.toArray
  }

  def kNN(query: Point, distFunc: (Point, MBR) => Double, k: Int, keepSame: Boolean): Array[(MBR, Int)] = {
    val ans = mutable.ArrayBuffer[(MBR, Int)]()
    val pq = new mutable.PriorityQueue[(_, Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((root, 0.0))

    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis))
          break()

        now._1 match {
          case node: RTreeNode =>
            node.m_child.foreach(entry =>
              if (node.isLeaf) pq.enqueue((entry, distFunc(query, entry.region)))
              else pq.enqueue((entry.node, distFunc(query, entry.region)))
            )
          case entry: RTreeEntry =>
            cnt += entry.nodeSize
            kNN_dis = now._2
            ans += ((entry.region, entry.m_data))
        }
      }
    }

    ans.toArray
  }

  def kNN(query: MBR, distFunc: (MBR, MBR) => Double, k: Int, keepSame: Boolean): Array[(MBR, Int)] = {
    val ans = mutable.ArrayBuffer[(MBR, Int)]()
    val pq = new mutable.PriorityQueue[(_, Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((root, 0.0))

    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis))
          break()

        now._1 match {
          case node: RTreeNode =>
            node.m_child.foreach(entry =>
              if (node.isLeaf) pq.enqueue((entry, distFunc(query, entry.region)))
              else pq.enqueue((entry.node, distFunc(query, entry.region)))
            )
          case entry: RTreeEntry =>
            cnt += entry.nodeSize
            kNN_dis = now._2
            ans += ((entry.region, entry.m_data))
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
    for (i <- 0 to dimension - 1) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0/(dimension - i))).toInt
      remaining /= dim(i)
    }

    def recursiveGroupPoint(entries: Array[(Point, Int)], cur_dim: Int, until_dim: Int) : Array[Array[(Point, Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(_._1.coord(cur_dim) < _._1.coord(cur_dim))
                           .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim)
        grouped.map(now => recursiveGroupPoint(now, cur_dim + 1, until_dim)).flatMap(list => list)
      else grouped
    }

    val grouped = recursiveGroupPoint(entries, 0, dimension - 1)
    val rtree_nodes = mutable.ArrayBuffer[(MBR, RTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(x => Double.MaxValue)
      val max = new Array[Double](dimension).map(x => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.coord(i))
        for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.coord(i))
      })
      val mbr = new MBR(new Point(min), new Point(max))
      rtree_nodes += ((mbr, new RTreeNode(mbr, list)))
    })

    var cur_rtree_nodes = rtree_nodes.toArray
    var cur_len = cur_rtree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 to dimension - 1) {
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
      val grouped = entries.sortWith(comp(cur_dim)).grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim)
        grouped.map(now => recursiveGroupRTreeNode(now, cur_dim + 1, until_dim)).flatMap(list => list)
      else grouped
    }

    while (!over(dim)) {
      val grouped = recursiveGroupRTreeNode(cur_rtree_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(MBR, RTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(x => Double.MaxValue)
        val max = new Array[Double](dimension).map(x => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
          for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
        })
        val mbr = new MBR(new Point(min), new Point(max))
        tmp_nodes += ((mbr, new RTreeNode(mbr, list)))
      })
      cur_rtree_nodes = tmp_nodes.toArray
      cur_len = cur_rtree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 to dimension - 1) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }

    val min = new Array[Double](dimension).map(x => Double.MaxValue)
    val max = new Array[Double](dimension).map(x => Double.MinValue)
    cur_rtree_nodes.foreach(now => {
      for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
      for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
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
    for (i <- 0 to dimension - 1) {
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
      val grouped = entries.sortWith(compMBR(cur_dim)).grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
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
        for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
        for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
      })
      val mbr = new MBR(new Point(min), new Point(max))
      rtree_nodes += ((mbr, new RTreeNode(mbr, list)))
    })

    var cur_rtree_nodes = rtree_nodes.toArray
    var cur_len = cur_rtree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 to dimension - 1) {
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

    def recursiveGroupRTreeNode(entries: Array[(MBR, RTreeNode)], cur_dim : Int, until_dim : Int) : Array[Array[(MBR, RTreeNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim)).grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
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
          for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
          for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
        })
        val mbr = new MBR(new Point(min), new Point(max))
        tmp_nodes += ((mbr, new RTreeNode(mbr, list)))
      })
      cur_rtree_nodes = tmp_nodes.toArray
      cur_len = cur_rtree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 to dimension - 1) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }

    val min = new Array[Double](dimension).map(x => Double.MaxValue)
    val max = new Array[Double](dimension).map(x => Double.MinValue)
    cur_rtree_nodes.foreach(now => {
      for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
      for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
    })

    val mbr = new MBR(new Point(min), new Point(max))
    val root = new RTreeNode(mbr, cur_rtree_nodes)
    new RTree(root)
  }
}
