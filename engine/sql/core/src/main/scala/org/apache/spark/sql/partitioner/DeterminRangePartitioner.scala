package org.apache.spark.sql.partitioner

import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.{SparkConf, SparkEnv, Partitioner}
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.util.{MutablePair, CollectionsUtils}
import org.apache.spark.sql.Row

/**
 * Created by crystalove on 15-5-27.
 */

object determinRangePartition extends DataPartition{
    def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]
    def apply[T](originRDD: RDD[(Double, (T, Row))], rangeBounds: Array[Double]) : RDD[(Double, (T, Row))] = {
        val rdd = if (sortBasedShuffleOn) {
            originRDD.mapPartitions { iter => iter.map(row => {
                //val tmp = row.copy()
                (row._1, (row._2._1, row._2._2.copy()))
            })}
        } else {
            originRDD.mapPartitions { iter =>
                val mutablePair = new MutablePair[Double, (T, Row)]()
                iter.map(row => mutablePair.update(row._1, (row._2._1, row._2._2.copy())))
            }
        }

        val part = new DeterminRangePartitioner(rangeBounds, ascending = true)
        val shuffled = new ShuffledRDD[Double, (T, Row), (T, Row)](rdd, part)
        shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
        shuffled
    }
}

class DeterminRangePartitioner(rangeBounds: Array[Double], ascending: Boolean) extends Partitioner {
    def numPartitions = rangeBounds.length + 1

    private val binarySearch: ((Array[Double], Double) => Int) = CollectionsUtils.makeBinarySearch[Double]

    def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[Double]
        var partition = 0
        if (rangeBounds.length <= 128) {
            while (partition < rangeBounds.length && Ordering[Double].gt(k, rangeBounds(partition))) {
                partition += 1
            }
        } else {
            partition = binarySearch(rangeBounds, k)
            if (partition < 0) {
                partition = -partition-1
            }
            if (partition > rangeBounds.length) {
                partition = rangeBounds.length
            }
        }
        if (ascending) {
            partition
        } else {
            rangeBounds.length - partition
        }
    }
}
