package org.apache.spark.sql.partitioner

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.util.MutablePair
import org.apache.spark.{SparkConf, Partitioner, SparkEnv}
import org.apache.spark.sql.catalyst.expressions.Row

/**
 * Created by crystalove on 15-5-26.
 */
object mapDPartition extends DataPartition{
    def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

    def apply[T](originRDD: RDD[(Int, (T, Row))], num_partitions: Int)
    : RDD[(Int, (T, Row))] = {
        val rdd = if (sortBasedShuffleOn) {
            originRDD.mapPartitions { iter => iter.map(row => (row._1, (row._2._1, row._2._2.copy())))}
        } else {
            originRDD.mapPartitions { iter =>
                val mutablePair = new MutablePair[Int, (T, Row)]
                iter.map(row => mutablePair.update(row._1, (row._2._1, row._2._2.copy())))
            }
        }

        val part = new MapDPartitioner(num_partitions)
        val shuffled = new ShuffledRDD[Int, (T, Row), (T, Row)](rdd, part)
        shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))

        shuffled
    }
}

class MapDPartitioner(num_partitions: Int) extends Partitioner {
    def numPartitions = num_partitions

    def getPartition(key: Any): Int = {
        key.asInstanceOf[Int]
    }
}
