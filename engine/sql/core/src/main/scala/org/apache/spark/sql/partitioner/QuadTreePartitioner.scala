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

package org.apache.spark.sql.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkEnv}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.index.{QuadTree, QuadTreeNode, RTree}
import org.apache.spark.sql.spatial.{MBR, Point, Shape}
import org.apache.spark.util.{MutablePair, SizeEstimator}

import scala.collection.mutable

/**
  * Created by gefei on 16-6-8.
  * A multi-demensional Data Partitioner based on QuadTree Algorithm
  */
object QuadTreePartitioner {
  def sortBasedShuffleOn: Boolean = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply(origin: RDD[(Point, InternalRow)], dimension: Int, est_partition: Int,
            sample_rate: Double, transfer_threshold: Long)
  : (RDD[(Point, InternalRow)], Array[(MBR, Int)], QuadTree) = {
    val rdd = if (sortBasedShuffleOn) {
      origin.mapPartitions { iter => iter.map(row => (row._1, row._2.copy())) }
    } else {
      origin.mapPartitions { iter =>
        val mutablePair = new MutablePair[Point, InternalRow]()
        iter.map(row => mutablePair.update(row._1, row._2.copy()))
      }
    }

    val part = new QuadTreePartitioner(est_partition, sample_rate, dimension,
      transfer_threshold, rdd)
    val shuffled = new ShuffledRDD[Point, InternalRow, InternalRow](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
    (shuffled, part.mbrBound, part.global_index)
  }
}

class QuadTreePartitioner(est_partition: Int,
                          sample_rate: Double,
                          dimension: Int,
                          transfer_threshold: Long,
                          rdd: RDD[_ <: Product2[Point, Any]])
  extends Partitioner {
  private case class Bounds(min: Array[Double], max: Array[Double])

  require(dimension == 2, "Only two dimensions are supported for a QuadTree")

//  val root_qtree_node: QuadTreeNode = null

  var (mbrBound, partitions, global_index) = {
    val (data_bounds, total_size, num_of_records) = {
      rdd.aggregate[(Bounds, Long, Int)]((null, 0, 0))((bound, data) => {
        val new_bound = if (bound._1 == null) {
          new Bounds(data._1.coord, data._1.coord)
        } else {
          new Bounds(bound._1.min.zip(data._1.coord).map(x => Math.min(x._1, x._2)),
            bound._1.max.zip(data._1.coord).map(x => Math.max(x._1, x._2)))
        }
        (new_bound, bound._2 + SizeEstimator.estimate(data._1), bound._3 + 1)
      }, (left, right) => {
        val new_bound = {
          if (left._1 == null) right._1
          else if (right._1 == null) left._1
          else {
            new Bounds(left._1.min.zip(right._1.min).map(x => Math.min(x._1, x._2)),
              left._1.max.zip(right._1.max).map(x => Math.max(x._1, x._2)))
          }
        }
        (new_bound, left._2 + right._2, left._3 + right._3)
      })
    } // get the partition bound and the total size of a MBR

    // every node in QuadTree has a threshold of max number of entries
    val max_entries_per_node = num_of_records / est_partition * 3

    val seed = System.currentTimeMillis()
    val sampled = if (total_size * sample_rate <= transfer_threshold){
      rdd.sample(withReplacement = false, sample_rate, seed).map(_._1).collect()
    }
    else {
      rdd.sample(withReplacement = true, transfer_threshold / total_size, seed).map(_._1).collect()
    }

    var count = 0
    val tmp_qtree = QuadTree(sampled.zipWithIndex,
      (data_bounds.min.head, data_bounds.min(1), data_bounds.max.head, data_bounds.max(1)))

    def searchMBROnQuadTree(node: QuadTreeNode): Array[(MBR, Int)] = {
      val ans = mutable.ArrayBuffer[(MBR, Int)]()
      if (node.children == null){
        val mbr = new MBR(Point(Array(node.x_low, node.y_low)),
          Point(Array(node.x_high, node.y_high)))
        ans += (mbr -> count)
        node.objects = Array((mbr.centroid.coord(0), mbr.centroid.coord(1), count))
        count += 1
      } else for (child <- node.children) ans ++= searchMBROnQuadTree(child)
      ans.toArray
    }

    val mbrs = searchMBROnQuadTree(tmp_qtree.root)
    (mbrs, mbrs.length, tmp_qtree)
  }

  val rt = RTree(mbrBound.map(x => (x._1, x._2, 1)), 25) // use the default value is fine

  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Point]
    rt.circleRange(k, 0.0).head._2
  }

}