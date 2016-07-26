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

package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.index.RTree
import org.apache.spark.sql.partitioner.{MapDPartition, STRPartition}
import org.apache.spark.sql.spatial._

import scala.collection.mutable

/**
  * Created by dong on 1/20/16.
  * KNN Join based on Two-Level R-Tree Structure
  */
case class RKJSpark(left_key: Expression,
                    right_key: Expression,
                    l: Literal,
                    left: SparkPlan,
                    right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = sqlContext.conf.numShufflePartitions
  final val sample_rate = sqlContext.conf.sampleRate
  final val transfer_threshold = sqlContext.conf.transferThreshold
  final val theta_boost = sqlContext.conf.thetaBoost
  final val max_entries_per_node = sqlContext.conf.maxEntriesPerNode
  final val k = l.value.asInstanceOf[Number].intValue()

  override protected def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute().map(row =>
      (BindReferences.bindReference(left_key, left.output).eval(row)
          .asInstanceOf[Point], row)
    )

    val right_rdd = right.execute().map(row =>
      (BindReferences.bindReference(right_key, right.output).eval(row)
        .asInstanceOf[Point], row)
    )

    val right_sampled = right_rdd
      .sample(withReplacement = false, sample_rate, System.currentTimeMillis())
      .map(_._1).collect().zipWithIndex
    val right_rt = RTree(right_sampled, max_entries_per_node)
    val dimension = right_sampled.head._1.coord.length

    val (left_partitioned, left_mbr_bound) =
      STRPartition(left_rdd, dimension, num_partitions, sample_rate,
        transfer_threshold, max_entries_per_node)

    val dim = new Array[Int](dimension)
    var remaining = theta_boost.toDouble
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    val refined_mbr_bound = left_partitioned.mapPartitionsWithIndex {(id, iter) =>
      if (iter.hasNext) {
        val data = iter.map(_._1).toArray
        def recursiveGroupPoint(entries: Array[Point], cur_dim: Int, until_dim: Int)
        : Array[(Point, Double)] = {
          val len = entries.length.toDouble
          val grouped = entries.sortWith(_.coord(cur_dim) < _.coord(cur_dim))
            .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
          if (cur_dim < until_dim) {
            grouped.map(now => recursiveGroupPoint(now, cur_dim + 1, until_dim))
              .flatMap(list => list)
          } else grouped.map {list =>
            val min = new Array[Double](dimension).map(x => Double.MaxValue)
            val max = new Array[Double](dimension).map(x => Double.MinValue)
            list.foreach { now =>
              for (i <- min.indices) min(i) = Math.min(min(i), now.coord(i))
              for (i <- max.indices) max(i) = Math.max(max(i), now.coord(i))
            }
            val mbr = new MBR(new Point(min), new Point(max))
            var cur_max = 0.0
            list.foreach(now => {
              val cur_dis = mbr.centroid.minDist(now)
              if (cur_dis > cur_max) cur_max = cur_dis
            })
            (mbr.centroid, cur_max)
          }
        }
        recursiveGroupPoint(data, 0, dimension - 1).map(x => (x._1, x._2, id)).iterator
      } else Array().iterator
    }.collect()

    val theta = new Array[Double](refined_mbr_bound.length)
    for (i <- refined_mbr_bound.indices) {
      val query = refined_mbr_bound(i)._1
      val knn_mbr_ans = right_rt.kNN(query, k, keepSame = false)
      theta(i) = knn_mbr_ans.last._1.minDist(query) + (refined_mbr_bound(i)._2 * 2.0)
    }

    val bc_theta = sparkContext.broadcast(theta)

    val right_dup = right_rdd.flatMap(x => {
      var list = mutable.ListBuffer[(Int, (Point, InternalRow))]()
      val set = new mutable.HashSet[Int]()
      for (i <- refined_mbr_bound.indices) {
        val pid = refined_mbr_bound(i)._3
        if (!set.contains(pid) && refined_mbr_bound(i)._1.minDist(x._1) < bc_theta.value(i)) {
          list += ((pid, x))
          set += pid
        }
      }
      list
    })

    val right_dup_partitioned = MapDPartition(right_dup, left_mbr_bound.length).map(_._2)

    left_partitioned.zipPartitions(right_dup_partitioned) {
      (leftIter, rightIter) =>
        val ans = mutable.ListBuffer[InternalRow]()
        val right_data = rightIter.toArray
        if (right_data.length > 0) {
          val right_index = RTree(right_data.map(_._1).zipWithIndex, max_entries_per_node)
          leftIter.foreach(now =>
            ans ++= right_index.kNN(now._1, k, keepSame = false)
              .map(x => new JoinedRow(now._2, right_data(x._2)._2))
          )
        }
        ans.iterator
    }
  }
}
