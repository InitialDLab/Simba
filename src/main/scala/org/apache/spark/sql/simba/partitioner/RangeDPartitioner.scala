/*
 * Copyright 2017 by Simba Project
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

package org.apache.spark.sql.simba.partitioner

import org.apache.spark.util.CollectionsUtils
import org.apache.spark.{Partitioner, SparkEnv}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.MutablePair

import scala.reflect.ClassTag

/**
  * Created by dong on 1/15/16.
  * Range Partitoner with Determined Range Bounds
  */
object RangeDPartition {
  def sortBasedShuffleOn: Boolean = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply[K: Ordering: ClassTag, T](origin: RDD[(K, (T, InternalRow))],
                                      range_bounds: Array[K]): RDD[(K, (T, InternalRow))] = {
    val rdd = if (sortBasedShuffleOn) {
      origin.mapPartitions {iter => iter.map(row => (row._1, (row._2._1, row._2._2.copy())))}
    } else {
      origin.mapPartitions {iter =>
        val mutablePair = new MutablePair[K, (T, InternalRow)]()
        iter.map(row => mutablePair.update(row._1, (row._2._1, row._2._2.copy())))
      }
    }

    val part = new RangeDPartitioner(range_bounds, ascending = true)
    new ShuffledRDD[K, (T, InternalRow), (T, InternalRow)](rdd, part)
  }
}

class RangeDPartitioner[K: Ordering: ClassTag](range_bounds: Array[K],
                                               ascending: Boolean) extends Partitioner {
  def numPartitions: Int = range_bounds.length + 1

  private val binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (range_bounds.length < 128) {
      while (partition < range_bounds.length && Ordering[K].gt(k, range_bounds(partition)))
        partition += 1
    } else {
      partition = binarySearch(range_bounds, k)
      if (partition < 0) partition = -partition - 1
      if (partition > range_bounds.length) partition = range_bounds.length
    }
    if (ascending) partition
    else range_bounds.length - partition
  }
}