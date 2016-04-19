/*
 *  Copyright 2016 by Simba Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.sql.partitioner

import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.util.MutablePair
import org.apache.spark.{SparkConf, SparkEnv, Partitioner}
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.spatial.Point

/**
 * Created by dong on 15-5-31.
 */
object linearHashPartition extends DataPartition {
    def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

    def apply(originRDD: RDD[(Point, Row)], num_partitions: Int): RDD[(Point, Row)] = {
        val rdd = if (sortBasedShuffleOn) {
            originRDD.mapPartitions { iter => iter.map(row => (row.copy(), null))}
        } else {
            originRDD.mapPartitions { iter =>
                val mutablePair = new MutablePair[(Point, Row), Null]
                iter.map(row => mutablePair.update(row, null))
            }
        }

        val part = new LinearHashPartitioner(num_partitions)
        val shuffled = new ShuffledRDD[(Point, Row), Null, Null](rdd, part)
        shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))

        shuffled.map(_._1)
    }
}

class LinearHashPartitioner(num_partitions: Int) extends Partitioner {
    def numPartitions = num_partitions

    def getPartition(key: Any): Int = {
        (key.asInstanceOf[(Point, Row)]._1.hashCode() % numPartitions).toInt
    }

}
