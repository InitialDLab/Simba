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

import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.{Partitioner, SparkEnv}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.MutablePair

/**
  * Created by dong on 1/20/16.
  * Voronoi Partitioner which assigns points to its nearest pivot
  */
object VoronoiPartition {
  def sortBasedShuffleOn: Boolean = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply(origin: RDD[(Int, (Point, InternalRow))], pivot_to_group: Array[Int], num_group: Int)
  : RDD[(Int, (Point, InternalRow))] = {
    val rdd = if (sortBasedShuffleOn) {
      origin.mapPartitions {iter => iter.map(row => (row._1, (row._2._1, row._2._2.copy())))}
    } else {
      origin.mapPartitions {iter =>
        val mutablePair = new MutablePair[Int, (Point, InternalRow)]()
        iter.map(row => mutablePair.update(row._1, (row._2._1, row._2._2.copy())))
      }
    }

    val part = new VoronoiPartitioner(pivot_to_group, num_group)
    new ShuffledRDD[Int, (Point, InternalRow), (Point, InternalRow)](rdd, part)
  }
}

class VoronoiPartitioner(pivot_to_group: Array[Int], num_group: Int) extends Partitioner {
  override def numPartitions: Int = num_group

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    pivot_to_group(k)
  }
}
