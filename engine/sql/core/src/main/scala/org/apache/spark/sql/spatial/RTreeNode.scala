package org.apache.spark.sql.spatial

import scala.collection.mutable.ListBuffer

/**
 * Created by Dong Xie on 5/16/2015.
 * An R-Tree node for high dimensional data indexing
 */

case class RTreeEntry(region : Either[Point, MBR], node: RTreeNode, m_data : Int, nodeSize : Int)

case class RTreeNode(m_mbr : MBR, m_child: Array[RTreeEntry], isLeaf: Boolean) {
  def this(m_mbr : MBR, children : Either[Array[(Point, Int)], Array[(MBR, RTreeNode)]]) = {
    this(m_mbr, {
      var ans = ListBuffer[RTreeEntry]()
      children match{
        case Left(now) =>
          for (i <- 0 to now.length - 1)
            ans += new RTreeEntry(Left(now(i)._1), null, now(i)._2, 1)
        case Right(now) =>
          for (i <- 0 to now.length - 1)
            ans += new RTreeEntry(Right(now(i)._1), now(i)._2, -1, -1)
      }
      ans.toArray
    }, {children match {case Left(x) => true case Right(x) => false}})
  }

  def this(m_mbr : MBR, children : Either[Array[(MBR, Int, Int)], Array[(MBR, RTreeNode)]], isLeaf : Boolean) = {
    this(m_mbr, {
      var ans = ListBuffer[RTreeEntry]()
      children match{
        case Left(now) =>
          for (i <- 0 to now.length - 1)
            ans += new RTreeEntry(Right(now(i)._1), null, now(i)._2, now(i)._3)
        case Right(now) =>
          for (i <- 0 to now.length - 1)
            ans += new RTreeEntry(Right(now(i)._1), now(i)._2, -1, -1)
      }
      ans.toArray
    }, isLeaf)
  }
}