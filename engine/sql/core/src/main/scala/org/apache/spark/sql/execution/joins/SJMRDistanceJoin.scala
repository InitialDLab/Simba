package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.partitioner.{mapDPartition, strRangePartition}
import org.apache.spark.sql.spatial.{RTree, Point}

import scala.collection.mutable.ListBuffer

/**
 * Created by Dong on 7/12/2015.
 */
case class SJMRDistanceJoin(leftKeys: Seq[Expression],
                            rightKeys: Seq[Expression],
                            distance: Literal,
                            left: SparkPlan,
                            right: SparkPlan)
  extends BinaryNode {
  override def output = left.output ++ right.output

  //parameters which set in sqlContext.conf
  final val num_shuffle_partitions = sqlContext.conf.numShufflePartitions
  final val sample_rate = sqlContext.conf.sampleRate
  final val max_entries_per_node = sqlContext.conf.maxEntriesPerNode
  final val r = distance.toString.toDouble
  final val dimension = leftKeys.length

  override def execute() = {
    val leftRDD = left.execute().map(row => {
      val tmp : Array[Double] = new Array[Double](dimension)
      for (i <-0 to dimension - 1)
        tmp(i) = BindReferences.bindReference(leftKeys(i), left.output).eval(row).asInstanceOf[Number].doubleValue()
      (Point(tmp, dimension), row)
    })
    val rightRDD = right.execute().map(row => {
      val tmp : Array[Double] = new Array[Double](dimension)
      for (i <-0 to dimension - 1)
        tmp(i) = BindReferences.bindReference(rightKeys(i), right.output).eval(row).asInstanceOf[Number].doubleValue()
      (Point(tmp, dimension), row)
    })

    val (leftPartitioned, left_mbr_bound) = strRangePartition(leftRDD, dimension, num_shuffle_partitions, check_bound = true, sample_rate)
    val (rightPartitioned, right_mbr_bound) = strRangePartition(rightRDD, dimension, num_shuffle_partitions, check_bound = true, sample_rate)

    val right_rt = RTree.buildFromMBR(right_mbr_bound.zip(Array.fill[Int](right_mbr_bound.length)(0))
      .map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)

    val left_dup = new Array[Array[Int]](left_mbr_bound.length)
    val right_dup = new Array[Array[Int]](right_mbr_bound.length)

    var tot = 0
    left_mbr_bound.foreach(now => {
      val res = right_rt.circleRangeMBR(now._1, r)
      left_dup(now._2) = Array()
      res.foreach(x => {
        if (right_dup(x._2) == null) right_dup(x._2) = Array(tot)
        else right_dup(x._2) = right_dup(x._2) :+ tot
        left_dup(now._2) = left_dup(now._2) :+ tot
        tot += 1
      })
    })

    val bc_left_dup = sparkContext.broadcast(left_dup)
    val bc_right_dup = sparkContext.broadcast(right_dup)

    val leftDuplicated = leftPartitioned.mapPartitionsWithIndex[(Int, (Point, Row))]((id, iter) => iter.flatMap(now => {
      if (bc_left_dup.value(id) != null) bc_left_dup.value(id).map(x => (x, now))
      else Array[(Int, (Point, Row))]()
    }))

    val rightDuplicated = rightPartitioned.mapPartitionsWithIndex[(Int, (Point, Row))]((id, iter) => iter.flatMap(now => {
      if (bc_right_dup.value(id) != null) bc_right_dup.value(id).map(x => (x, now))
      else Array[(Int, (Point, Row))]()
    }))

    val leftDupPartitioned = mapDPartition(leftDuplicated, tot).map(_._2)
    val rightDupPartitioned = mapDPartition(rightDuplicated, tot).map(_._2)

    leftDupPartitioned.zipPartitions(rightDupPartitioned) {
      (leftIter, rightIter) => {
        val ans = ListBuffer[Row]()
        val right_data = rightIter.toArray
        if (right_data.length > 0) {
          val right_index = RTree(right_data.map(_._1).zipWithIndex, dimension, max_entries_per_node)
          while (leftIter.hasNext) {
            val now = leftIter.next()
            val query = now._1
            val cur_ans = right_index.circleRange(query, r)
            cur_ans.foreach(x =>
              ans += new JoinedRow(now._2, right_data(x._2)._2)
            )
          }
        }
        ans.iterator
      }
    }
  }
}