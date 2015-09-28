package org.apache.spark.sql.partitioner

import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.util.MutablePair
import org.apache.spark.{SparkConf, SparkEnv, Partitioner}
import org.apache.spark.sql.spatial.Point
import org.apache.spark.sql.catalyst.InternalRow

/**
 * Created by Dong Xie on 9/28/15.
 * Voronoi Partitioner which assigns points to its nearest pivot
 */
object VoronoiPartition {
  def sortBasedShuffledOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

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
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
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
