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

import org.apache.spark.{Partitioner, SparkEnv}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.MutablePair

/**
  * Created by dong on 1/15/16.
  * Linear Hash Partitioner with Java hashcode
  */
object HashPartition {
  def sortBasedShuffleOn: Boolean = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply(origin: RDD[(Any, InternalRow)], num_partitions: Int): RDD[(Any, InternalRow)] = {
    val rdd = if (sortBasedShuffleOn) {
      origin.mapPartitions {iter => iter.map(row => (row._1, row._2.copy()))}
    } else {
      origin.mapPartitions {iter =>
        val mutablePair = new MutablePair[Any, InternalRow]()
        iter.map(row => mutablePair.update(row._1, row._2.copy()))
      }
    }

    val part = new HashPartitioner(num_partitions)
    new ShuffledRDD[Any, InternalRow, InternalRow](rdd, part)
  }
}

class HashPartitioner(num_partitions: Int) extends Partitioner {
  override def numPartitions: Int = num_partitions

  override def getPartition(key: Any): Int = {
    key.hashCode() % num_partitions
  }
}