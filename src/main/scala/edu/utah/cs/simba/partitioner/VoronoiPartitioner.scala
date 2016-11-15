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

import edu.utah.cs.simba.spatial.Point
import edu.utah.cs.simba.util.SimbaSerializer
import org.apache.spark.{Partitioner, SparkConf, SparkEnv}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.MutablePair

/**
  * Created by dong on 1/20/16.
  * Voronoi Partitioner which assigns points to its nearest pivot
  */
object VoronoiPartition {
  def sortBasedShuffledOn: Boolean = SparkEnv.get.conf.get("spark.shuffle.manager", "sort") != "hash"

  def apply(origin: RDD[(Int, (Point, InternalRow))], pivot_to_group: Array[Int], num_group: Int)
  : RDD[(Int, (Point, InternalRow))] = {
    val rdd = if (sortBasedShuffledOn) {
      origin.mapPartitions {iter => iter.map(row => (row._1, (row._2._1, row._2._2.copy())))}
    } else {
      origin.mapPartitions {iter =>
        val mutablePair = new MutablePair[Int, (Point, InternalRow)]()
        iter.map(row => mutablePair.update(row._1, (row._2._1, row._2._2.copy())))
      }
    }

    val part = new VoronoiPartitioner(pivot_to_group, num_group)
    val shuffled = new ShuffledRDD[Int, (Point, InternalRow), (Point, InternalRow)](rdd, part)
    shuffled.setSerializer(new SimbaSerializer(new SparkConf(false)))
    shuffled
  }
}

class VoronoiPartitioner(pivot_to_group: Array[Int], num_group: Int) extends Partitioner {
  override def numPartitions: Int = num_group

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    pivot_to_group(k)
  }
}
