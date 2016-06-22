/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.spark.sql.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkEnv}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.index.RTree
import org.apache.spark.sql.spatial.{MBR, Point, Shape}
import org.apache.spark.util.{MutablePair, SizeEstimator}

import scala.collection.mutable

/**
  * Created by gefei on 16-6-8.
  * A Multi-Dimensional Data Partitioner based on KDTree Algorithm
  */
object KDTreePartitioner {
  def sortBasedShuffleOn: Boolean = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply(origin: RDD[(Point, InternalRow)], dimension: Int, est_partition: Int,
            sample_rate: Double, transfer_threshold: Long)
  : (RDD[(Point, InternalRow)], Array[(MBR, Int)]) = {
    val rdd = if (sortBasedShuffleOn) {
      origin.mapPartitions { iter => iter.map(row => (row._1, row._2.copy())) }
    } else {
      origin.mapPartitions { iter =>
        val mutablePair = new MutablePair[Point, InternalRow]()
        iter.map(row => mutablePair.update(row._1, row._2.copy()))
      }
    }

    val part = new KDTreePartitioner(est_partition, sample_rate, dimension,
      transfer_threshold, rdd)
    val shuffled = new ShuffledRDD[Point, InternalRow, InternalRow](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
    (shuffled, part.mbrBound)
  }

}
class KDTreePartitioner(est_partition: Int,
                          sample_rate: Double,
                          dimension: Int,
                          transfer_threshold: Long,
                          rdd: RDD[_ <: Product2[Point, Any]])
  extends Partitioner {
  private case class Bounds(min: Array[Double], max: Array[Double])

  var (mbrBound, partitions) = {
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

    val max_entries_per_node = num_of_records / est_partition

    val seed = System.currentTimeMillis()
    val sampled = if (total_size * sample_rate <= transfer_threshold){
      rdd.sample(withReplacement = false, sample_rate, seed).map(_._1).collect()
    }
    else {
      rdd.sample(withReplacement = true, transfer_threshold / total_size, seed).map(_._1).collect()
    }

    def recursiveGroupPoint(entries: Array[Point], low_bound: Seq[Double],
                            high_bound: Seq[Double], cur_dim: Int): Array[MBR] = {
      var ans = mutable.ArrayBuffer[MBR]()
      val grouped = entries.sortWith((a, b) =>
        a.coord(cur_dim) < b.coord(cur_dim)).grouped(Math.ceil(entries.length / 2.0).toInt).toArray
      val center = grouped(1).head.coord
      require(grouped.length == 2)

      val new_high = 0 until dimension map {i =>
        if (i != cur_dim) high_bound(i)
        else center(i)
      }
      val new_low = 0 until dimension map { i =>
        if (i != cur_dim) low_bound(i)
        else center(i)
      }
      if (grouped(0).length >= max_entries_per_node){
        ans ++= recursiveGroupPoint(grouped(0), low_bound,
          new_high, (cur_dim + 1) % dimension)
      } else {
        ans += new MBR(new Point(low_bound.toArray.clone()),
          new Point(new_high.toArray.clone()))
      }
      if (grouped(1).length >= max_entries_per_node){
        ans ++= recursiveGroupPoint(grouped(1), new_low,
          high_bound, (cur_dim + 1) % dimension)
      } else {
        ans += new MBR(new Point(new_low.toArray.clone()),
          new Point(high_bound.toArray.clone()))
      }
      ans.toArray
    }

    val mbrs = recursiveGroupPoint(sampled, data_bounds.min, data_bounds.max, 0)
    (mbrs.zipWithIndex, mbrs.length)
  }

  val rt = RTree(mbrBound.map(x => (x._1, x._2, 1)), 25) // the default value is fine

  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Point]
    rt.circleRange(k, 0.0).head._2
  }
}
