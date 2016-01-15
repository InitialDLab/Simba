package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Partitioning}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.spatial.{RTree, MBR, Point}
import org.apache.spark.sql.partitioner.{mapDPartition, strRangePartition}
import org.apache.spark.util.MutablePair
import org.apache.spark.{SparkEnv, Partitioner}

import scala.collection.mutable.ListBuffer

/**
 *  Created by crystalove on 15-5-22.
 */
case class RTreeDistanceJoin(leftKeys: Seq[Expression],
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

    val leftPartitionSize = leftPartitioned.mapPartitions{iter => new Iterator[Int] {
      override def hasNext = iter.hasNext
      override def next() = {
        iter.length
      }
    }}.collect()

    val left_rt = RTree.buildFromMBR(left_mbr_bound.zip(leftPartitionSize).map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)
    val bc_rt = sparkContext.broadcast(left_rt)

    val rightDuplicated = rightRDD.flatMap(p => {
      var list = ListBuffer[(Int, (Point, Row))]()
      bc_rt.value.circleRangeMBR(p._1, r).foreach(x => list += ((x._2, p)))
      list
    })

    val rightDupPartioned = mapDPartition(rightDuplicated, left_mbr_bound.length)

    leftPartitioned.zipPartitions(rightDupPartioned) {
      (leftIter, rightIter) => {
        val ans = ListBuffer[Row]()
        val right_data = rightIter.map(row => row._2).toArray
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
