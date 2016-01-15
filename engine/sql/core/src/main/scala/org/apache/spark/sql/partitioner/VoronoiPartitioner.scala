package org.apache.spark.sql.partitioner

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.spatial.Point
import org.apache.spark.util.MutablePair
import org.apache.spark.{SparkConf, Partitioner, SparkEnv}

/**
 * Created by gefei on 15-6-30.
 */

object voronoiPartition{
  def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply(originRDD: RDD[(Int, (Point, Row))], pivot_to_group : Array[Int], num_group: Int) : RDD[(Int, (Point, Row))] = {
    val rdd = if (sortBasedShuffleOn) {
      originRDD.mapPartitions { iter => iter.map(row => (row._1, (row._2._1, row._2._2.copy()))) }
    } else {
      originRDD.mapPartitions { iter =>
        val mutablePair = new MutablePair[Int, (Point, Row)]
        iter.map(row => mutablePair.update(row._1, (row._2._1, row._2._2.copy())))
      }
    }

    val part = new VoronoiPartitioner(pivot_to_group, num_group)
    val shuffled = new ShuffledRDD[Int, (Point, Row), (Point, Row)](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
    shuffled
  }
}

class VoronoiPartitioner(pivot_to_group: Array[Int], num_group: Int) extends Partitioner {
  def numPartitions = num_group

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    pivot_to_group(k)
  }
}