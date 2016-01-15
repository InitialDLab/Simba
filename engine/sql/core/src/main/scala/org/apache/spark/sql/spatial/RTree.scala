package org.apache.spark.sql.spatial

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.index.Index

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
 * Created by Dong Xie on 5/16/2015.
 * RTree Index
 */

class NNOrdering() extends Ordering[(Either[RTreeNode, RTreeEntry], Double)] {
  def compare(a: (Either[RTreeNode, RTreeEntry], Double), b: (Either[RTreeNode, RTreeEntry], Double)): Int = -a._2.compare(b._2)
}

case class RTree(root: RTreeNode) extends Index {
  def range(query: MBR): Array[(Point, Int)] = {
    var ans = ListBuffer[(Point, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.isIntersect(query) && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach(entry => {
          if (query.isIntersect(entry.region.right.get))
            st.push(entry.node)
        })
      } else {
        now.m_child.foreach(entry => {
          val p = entry.region.left.get
          if (query.contains(p))
            ans += ((p, entry.m_data))
        })
      }
    }
    ans.toArray
  }

  def rangeMBR(query: MBR): Array[(MBR, Int)] = {
    var ans = ListBuffer[(MBR, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.isIntersect(query) && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach(entry => {
          if (query.isIntersect(entry.region.right.get))
            st.push(entry.node)
        })
      } else {
        now.m_child.foreach(entry => {
          val p = entry.region.right.get
          if (query.isIntersect(p))
            ans += ((p, entry.m_data))
        })
      }
    }
    ans.toArray
  }

  def circleRange(origin: Point, r: Double): Array[(Point, Int)] = {
    var ans = ListBuffer[(Point, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach(entry => {
          if (origin.minDist(entry.region.right.get) <= r)
            st.push(entry.node)
        })
      } else {
        now.m_child.foreach(entry => {
          val p = entry.region.left.get
          if (origin.minDist(p) <= r)
            ans += ((p, entry.m_data))
        })
      }
    }
    ans.toArray
  }

  def circleRangeConjunctive(queries: Array[(Point, Double)]) : Array[(Point, Int)] = {
    var ans = ListBuffer[(Point, Int)]()
    val st = new mutable.Stack[RTreeNode]()

    def checkMBR(mbr: MBR) : Boolean = {
      for (i <- queries.indices)
        if (mbr.minDist(queries(i)._1) > queries(i)._2)
          return false
      true
    }

    def checkPoint(point: Point) : Boolean = {
      for (i <- queries.indices)
        if (point.minDist(queries(i)._1) > queries(i)._2)
          return false
      true
    }

    if (checkMBR(root.m_mbr) && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach(entry => {
          if (checkMBR(entry.region.right.get))
            st.push(entry.node)
        })
      } else {
        now.m_child.foreach(entry => {
          val p = entry.region.left.get
          if (checkPoint(p))
            ans += ((p, entry.m_data))
        })
      }
    }
    ans.toArray
  }

  def circleRangeMBRConjunctive(queries: Array[(Point, Double)]) : Array[(MBR, Int)] = {
    var ans = ListBuffer[(MBR, Int)]()
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
      if (!now.isLeaf) {
        now.m_child.foreach(entry => {
          if (checkMBR(entry.region.right.get))
            st.push(entry.node)
        })
      } else {
        now.m_child.foreach(entry => {
          val p = entry.region.right.get
          if (checkMBR(p))
            ans += ((p, entry.m_data))
        })
      }
    }
    ans.toArray
  }

  def circleRangeMBR(origin: MBR, r: Double): Array[(MBR, Int)] = {
    var ans = ListBuffer[(MBR, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach(entry => {
          if (origin.minDist(entry.region.right.get) <= r)
            st.push(entry.node)
        })
      } else {
        now.m_child.foreach(entry => {
          val p = entry.region.right.get
          if (origin.minDist(p) <= r)
            ans += ((p, entry.m_data))
        })
      }
    }
    ans.toArray
  }

  def circleRangeMBR(origin: Point, r: Double): Array[(MBR, Int)] = {
    var ans = ListBuffer[(MBR, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty)
      st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach(entry => {
          if (origin.minDist(entry.region.right.get) <= r)
            st.push(entry.node)
        })
      } else {
        now.m_child.foreach(entry => {
          val p = entry.region.right.get
          if (origin.minDist(p) <= r)
            ans += ((p, entry.m_data))
        })
      }
    }
    ans.toArray
  }

  def kNN(query : Point, k: Int, keepSame : Boolean) : Array[(Point, Int)] = {
    var ans = ListBuffer[(Point, Int)]()
    val pq = new mutable.PriorityQueue[(Either[RTreeNode, RTreeEntry], Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((Left(root), 0.0))
    val loop = new Breaks
    loop.breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis))
          loop.break()

        now._1 match {
          case Left(x) =>
            x.m_child.foreach(entry => {
              if (x.isLeaf) {
                val x = entry.region.left.get
                pq.enqueue((Right(entry), x.minDist(query)))
              } else {
                pq.enqueue((Left(entry.node), entry.region.right.get.minDist(query)))
              }
            })
          case Right(x) =>
            cnt += 1
            kNN_dis = now._2
            ans += ((x.region.left.get, x.m_data))
        }
      }
    }
    ans.toArray
  }

  def kNN(query : MBR, distFunc:(Point, MBR) => Double, distFuncMBR:(MBR, MBR) => Double, k : Int, keepSame: Boolean) : Array[(Point, Int)] = {
    var ans = ListBuffer[(Point, Int)]()
    val pq = new mutable.PriorityQueue[(Either[RTreeNode, RTreeEntry], Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((Left(root), 0.0))
    val loop = new Breaks
    loop.breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis))
          loop.break()

        now._1 match {
          case Left(x) =>
            x.m_child.foreach(entry => {
              if (x.isLeaf) {
                val x = entry.region.left.get
                pq.enqueue((Right(entry), distFunc(x, query)))
              } else {
                pq.enqueue((Left(entry.node), distFuncMBR(entry.region.right.get, query)))
              }
            })
          case Right(x) =>
            cnt += 1
            kNN_dis = now._2
            ans += ((x.region.left.get, x.m_data))
        }
      }
    }
    ans.toArray
  }

  def kNNMBR(query : MBR, distFunc : (MBR, MBR) => Double, k: Int, keepSame : Boolean) : Array[(MBR, Int)] = {
    var ans = ListBuffer[(MBR, Int)]()
    val pq = new mutable.PriorityQueue[(Either[RTreeNode, RTreeEntry], Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((Left(root), 0.0))
    val loop = new Breaks
    loop.breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis))
          loop.break()

        now._1 match {
          case Left(x) =>
            x.m_child.foreach(entry => {
              if (x.isLeaf) {
                val x = entry.region.right.get
                pq.enqueue((Right(entry), distFunc(query, x)))
              } else {
                pq.enqueue((Left(entry.node), distFunc(query, entry.region.right.get)))
              }
            })
          case Right(x) =>
            cnt += x.nodeSize
            kNN_dis = now._2
            ans += ((x.region.right.get, x.m_data))
        }
      }
    }
    ans.toArray
  }

  def kNNpMBR(query : Point, distFunc : (Point, MBR) => Double, k: Int, keepSame : Boolean) : Array[(MBR, Int)] = {
    var ans = ListBuffer[(MBR, Int)]()
    val pq = new mutable.PriorityQueue[(Either[RTreeNode, RTreeEntry], Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((Left(root), 0.0))
    val loop = new Breaks
    loop.breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis))
          loop.break()

        now._1 match {
          case Left(x) =>
            x.m_child.foreach(entry => {
              if (x.isLeaf) {
                val x = entry.region.right.get
                pq.enqueue((Right(entry), query.minDist(x)))
              } else {
                pq.enqueue((Left(entry.node), query.minDist(entry.region.right.get)))
              }
            })
          case Right(x) =>
            cnt += x.nodeSize
            kNN_dis = now._2
            ans += ((x.region.right.get, x.m_data))
        }
      }
    }
    ans.toArray
  }
}

object RTree {
  //val sqlContext = SparkPlan.currentContext.get()
//  final val max_entries_per_node = sqlContext.conf.maxEntriesPerNode

  def apply(entries : Array[(Point, Int)], dimension: Int, max_entries_per_node: Int) : RTree = {
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 to dimension - 1) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def recursiveGroupPoint(entries: Array[(Point, Int)], cur_dim : Int, until_dim : Int) : Array[Array[(Point, Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(_._1.coord(cur_dim) < _._1.coord(cur_dim)).grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.map(now => {
          recursiveGroupPoint(now, cur_dim + 1, until_dim)
        }).flatMap(list => list)
      } else grouped
    }

    val grouped = recursiveGroupPoint(entries, 0, dimension - 1)
    val rtree_nodes = ListBuffer[(MBR, RTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(x => Double.MaxValue)
      val max = new Array[Double](dimension).map(x => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.coord(i))
        for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.coord(i))
      })
      val mbr = new MBR(new Point(min), new Point(max))
      rtree_nodes += ((mbr, new RTreeNode(mbr, Left(list))))
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
      var tmp_nodes = ListBuffer[(MBR, RTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(x => Double.MaxValue)
        val max = new Array[Double](dimension).map(x => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
          for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
        })
        val mbr = new MBR(new Point(min), new Point(max))
        tmp_nodes += ((mbr, new RTreeNode(mbr, Right(list))))
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
    val root = new RTreeNode(mbr, Right(cur_rtree_nodes))
    new RTree(root)
  }

  def buildFromMBR(entries : Array[(MBR, Int, Int)], max_entries_per_node: Int) : RTree = {
    val dimension = entries(0)._1.low.dimensions
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 to dimension - 1) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def compMBRInt(dim: Int)(left : (MBR, Int, Int), right : (MBR, Int, Int)) : Boolean = {
      val left_center = left._1.low.coord(dim) + left._1.high.coord(dim)
      val right_center = right._1.low.coord(dim) + right._1.high.coord(dim)
      left_center < right_center
    }

    def recursiveGroupMBR(entries: Array[(MBR, Int, Int)], cur_dim : Int, until_dim : Int) : Array[Array[(MBR, Int, Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(compMBRInt(cur_dim)).grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.map(now => {
          recursiveGroupMBR(now, cur_dim + 1, until_dim)
        }).flatMap(list => list)
      } else grouped
    }

    val grouped = recursiveGroupMBR(entries, 0, dimension - 1)
    val rtree_nodes = ListBuffer[(MBR, RTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(x => Double.MaxValue)
      val max = new Array[Double](dimension).map(x => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
        for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
      })
      val mbr = new MBR(new Point(min), new Point(max))
      rtree_nodes += ((mbr, new RTreeNode(mbr, Left(list), true)))
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
      var tmp_nodes = ListBuffer[(MBR, RTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(x => Double.MaxValue)
        val max = new Array[Double](dimension).map(x => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now._1.low.coord(i))
          for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now._1.high.coord(i))
        })
        val mbr = new MBR(new Point(min), new Point(max))
        tmp_nodes += ((mbr, new RTreeNode(mbr, Right(list))))
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
    val root = new RTreeNode(mbr, Right(cur_rtree_nodes))
    new RTree(root)
  }
}