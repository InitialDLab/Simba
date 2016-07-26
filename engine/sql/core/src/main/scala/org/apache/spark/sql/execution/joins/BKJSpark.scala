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

package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.partitioner.MapDPartition
import org.apache.spark.sql.spatial._
import org.apache.spark.util.BoundedPriorityQueue

import scala.collection.mutable
import scala.util.Random

/**
  * Created by dong on 1/20/16.
  * KNN Join based on Block Nested Loop Approach
  */
case class BKJSpark(left_key: Expression,
                    right_key: Expression,
                    l: Literal,
                    left: SparkPlan,
                    right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = sqlContext.conf.numShufflePartitions
  final val k = l.value.asInstanceOf[Number].intValue()

  private class DisOrdering extends Ordering[(InternalRow, Double)] {
    override def compare(x : (InternalRow, Double), y: (InternalRow, Double)): Int =
      -x._2.compare(y._2)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val tot_rdd = left.execute().map((0, _)).union(right.execute().map((1, _)))

    val tot_dup_rdd = tot_rdd.flatMap {x =>
      val rand_no = new Random().nextInt(num_partitions)
      val ans = mutable.ListBuffer[(Int, (Int, InternalRow))]()
      if (x._1 == 0) {
        val base = rand_no * num_partitions
        for (i <- 0 until num_partitions)
          ans += ((base + i, x))
      } else {
        for (i <- 0 until num_partitions)
          ans += ((i * num_partitions + rand_no, x))
      }
      ans
    }

    val tot_dup_partitioned = MapDPartition(tot_dup_rdd, num_partitions * num_partitions)

    tot_dup_partitioned.mapPartitions {iter =>
      var left_data = mutable.ListBuffer[(Point, InternalRow)]()
      var right_data = mutable.ListBuffer[(Point, InternalRow)]()
      while (iter.hasNext) {
        val data = iter.next()
        if (data._2._1 == 0) {
          val tmp_point = BindReferences.bindReference(left_key, left.output).eval(data._2._2).
            asInstanceOf[Point]
          left_data += ((tmp_point, data._2._2))
        } else {
          val tmp_point = BindReferences.bindReference(right_key, right.output).eval(data._2._2).
            asInstanceOf[Point]
          right_data += ((tmp_point, data._2._2))
        }
      }

      val joined_ans = mutable.ListBuffer[(InternalRow, Array[(InternalRow, Double)])]()

      left_data.foreach(left => {
        var pq = new BoundedPriorityQueue[(InternalRow, Double)](k)(new DisOrdering)
        right_data.foreach(right => pq += ((right._2, right._1.minDist(left._1))))
        joined_ans += ((left._2, pq.toArray))
      })
      joined_ans.iterator
    }.reduceByKey((left, right) => (left ++ right).sortWith(_._2 < _._2).take(k), num_partitions)
    .flatMap {
      now => now._2.map(x => new JoinedRow(now._1, x._1))
    }
  }
}
