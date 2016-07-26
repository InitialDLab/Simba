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
import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.index.RTree
import org.apache.spark.sql.partitioner.MapDPartition
import org.apache.spark.sql.spatial._

import scala.collection.mutable
import scala.util.Random

/**
  * Created by dong on 1/20/16.
  * Distance Join based on Block Nested Loop + Local R-Tree
  */
case class BDJSparkR(left_key: Expression,
                     right_key: Expression,
                     l: Literal,
                     left: SparkPlan,
                     right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = sqlContext.conf.numShufflePartitions
  final val r = NumberConverter.literalToDouble(l)
  final val max_entries_per_node = sqlContext.conf.maxEntriesPerNode

  override protected def doExecute(): RDD[InternalRow] = {
    val tot_rdd = left.execute().map((0, _)).union(right.execute().map((1, _)))

    val tot_dup_rdd = tot_rdd.flatMap {x =>
      val rand_no = new Random().nextInt(num_partitions)
      var ans = mutable.ListBuffer[(Int, (Int, InternalRow))]()
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

      val joined_ans = mutable.ListBuffer[InternalRow]()

      if (right_data.nonEmpty) {
        val right_rtree = RTree(right_data.map(_._1).zipWithIndex.toArray, max_entries_per_node)
        left_data.foreach(left => right_rtree.circleRange(left._1, r)
          .foreach(x => joined_ans += new JoinedRow(left._2, right_data(x._2)._2)))
      }

      joined_ans.iterator
    }
  }
}
