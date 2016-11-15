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

package edu.utah.cs.simba.partitioner

import edu.utah.cs.simba.util.SimbaSerializer
import org.apache.spark.{Partitioner, SparkConf, SparkEnv}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.MutablePair

/**
  * Created by dong on 1/15/16.
  * Determined Key-Mapping Partitioner
  */
object MapDPartition {
  def sortBasedShuffleOn: Boolean = SparkEnv.get.conf.get("spark.shuffle.manager", "sort") != "hash"

  def apply[T](origin: RDD[(Int, (T, InternalRow))],
               num_partitions: Int): RDD[(Int, (T, InternalRow))] = {
    val rdd = if (sortBasedShuffleOn) {
      origin.mapPartitions {iter => iter.map(row => (row._1, (row._2._1, row._2._2.copy())))}
    } else {
      origin.mapPartitions {iter =>
        val mutablePair = new MutablePair[Int, (T, InternalRow)]()
        iter.map(row => mutablePair.update(row._1, (row._2._1, row._2._2.copy())))
      }
    }

    val part = new MapDPartitioner(num_partitions)
    val shuffled = new ShuffledRDD[Int, (T, InternalRow), (T, InternalRow)](rdd, part)
    shuffled.setSerializer(new SimbaSerializer(new SparkConf(false)))
    shuffled
  }
}

class MapDPartitioner(num_partitions: Int) extends Partitioner {
  def numPartitions: Int = num_partitions
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    require(k >= 0 && k < num_partitions)
    k
  }
}

