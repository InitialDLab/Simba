/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.simba.execution.join

import org.apache.spark.sql.simba.execution.SimbaPlan
import org.apache.spark.sql.simba.index.RTree
import org.apache.spark.sql.simba.partitioner.{MapDPartition, STRPartition}
import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.util.{NumberUtil, ShapeUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, Literal}
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

/**
  * Created by dong on 1/20/16.
  * Distance Join based on Two-Level R-Tree Structure
  */
case class RDJSpark(left_key: Expression, right_key: Expression, l: Literal,
                    left: SparkPlan, right: SparkPlan) extends SimbaPlan {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = simbaSessionState.simbaConf.joinPartitions
  final val sample_rate = simbaSessionState.simbaConf.sampleRate
  final val max_entries_per_node = simbaSessionState.simbaConf.maxEntriesPerNode
  final val transfer_threshold = simbaSessionState.simbaConf.transferThreshold
  final val r = NumberUtil.literalToDouble(l)

  override protected def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute().map(row =>
      (ShapeUtils.getShape(left_key, left.output, row).asInstanceOf[Point], row)
    )

    val right_rdd = right.execute().map(row =>
      (ShapeUtils.getShape(right_key, right.output, row).asInstanceOf[Point], row)
    )

    val dimension = right_rdd.first()._1.coord.length

    val (left_partitioned, left_mbr_bound) =
      STRPartition(left_rdd, dimension, num_partitions, sample_rate,
        transfer_threshold, max_entries_per_node)

    val left_part_size = left_partitioned.mapPartitions {
      iter => Array(iter.length).iterator
    }.collect()

    val left_rt = RTree(left_mbr_bound.zip(left_part_size).map(x => (x._1._1, x._1._2, x._2)),
      max_entries_per_node)
    val bc_rt = sparkContext.broadcast(left_rt)

    val right_dup = right_rdd.flatMap {x =>
      bc_rt.value.circleRange(x._1, r).map(now => (now._2, x))
    }

    val right_dup_partitioned = MapDPartition(right_dup, left_mbr_bound.length)

    left_partitioned.zipPartitions(right_dup_partitioned) {(leftIter, rightIter) =>
      val ans = mutable.ListBuffer[InternalRow]()
      val right_data = rightIter.map(_._2).toArray
      if (right_data.length > 0) {
        val right_index = RTree(right_data.map(_._1).zipWithIndex, max_entries_per_node)
        leftIter.foreach {now =>
          ans ++= right_index.circleRange(now._1, r)
            .map(x => new JoinedRow(now._2, right_data(x._2)._2))
        }
      }
      ans.iterator
    }
  }

  override def children: Seq[SparkPlan] = Seq(left, right)
}
