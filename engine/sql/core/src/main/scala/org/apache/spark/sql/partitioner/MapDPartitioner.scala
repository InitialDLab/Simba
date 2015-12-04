package org.apache.spark.sql.partitioner

import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.util.MutablePair
import org.apache.spark.{SparkConf, SparkEnv, Partitioner}

/**
 * Created by Dong Xie on 9/28/15.
 * Determined Key-Mapping Partitioner
 */

object MapDPartition {
  def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply[T](origin: RDD[(Int, (T, InternalRow))], num_partitions: Int): RDD[(Int, (T, InternalRow))] = {
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
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
    shuffled
  }
}

class MapDPartitioner(num_partitions: Int) extends Partitioner {
  def numPartitions = num_partitions
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    require(k >= 0 && k < num_partitions)
    k
  }
}
