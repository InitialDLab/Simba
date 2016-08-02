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
import org.apache.spark.sql.partitioner.{MapDPartition, RangeDPartition, RangePartition}
import org.apache.spark.sql.spatial.{Point, ZValue}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by dong on 1/20/16.
  * Approximate kNN Join based on Z-Value
  */
case class ZKJSpark(left_key: Expression,
                    right_key: Expression,
                    kNN: Literal,
                    left: SparkPlan,
                    right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  val k = kNN.toString.toInt
  // Parameters that set in sqlContext.conf
  val num_partition = sqlContext.conf.numShufflePartitions
  val num_shifts = sqlContext.conf.zknnShiftTimes

  private def genRandomShiftVectors(dimension : Int, shift : Int): Array[Array[Int]] = {
    val r = new Random(System.currentTimeMillis)
    val ans = Array.ofDim[Int](shift + 1, dimension)
    for (i <- 0 to shift)
      for (j <- 0 until dimension) {
        if (i == 0) ans(i)(j) = 0
        else ans(i)(j) = Math.abs(r.nextInt(100))
      }
    ans
  }

  private def calcShiftArray(point : Point, shift : Array[Int]) : Array[Int] = {
    val len = point.coord.length
    val ans = new Array[Int](len)

    for (i <- 0 until len)
      ans(i) = point.coord(i).toInt + shift(i)
    ans
  }

  private def binarySearch[T](array: Array[T], func: T => Double, key: Double): Int = {
    var left = 0
    var right = array.length - 1
    while (left < right) {
      val mid = (left + right) >> 1
      if (func(array(mid)) <= key) {
        left = mid + 1
      } else right = mid
    }
    left
  }

  def zKNNPerIter(left_rdd: RDD[(Point, InternalRow)], right_rdd: RDD[(Point, InternalRow)],
                  k: Int, shift: Array[Int]): RDD[(InternalRow, Array[(InternalRow, Double)])] = {
    val packed_left_rdd = left_rdd.map(row =>
      (ZValue(calcShiftArray(row._1, shift)).toDouble, row))

    val (left_partitioned_rdd, left_rdd_bound) = RangePartition(packed_left_rdd, num_partition)

    val packed_right_rdd = right_rdd.mapPartitions(iter => iter.map(row =>
      (ZValue(calcShiftArray(row._1, shift)).toDouble, row)))
    val right_partitioned_rdd = RangeDPartition(packed_right_rdd, left_rdd_bound)


    val right_rdd_with_indexed = right_partitioned_rdd.zipWithIndex()
    val right_rdd_bound = right_rdd_with_indexed.mapPartitions(iter => new Iterator[(Long, Long)] {
      def hasNext = iter.hasNext
      def next() = {
        val left = iter.next()._2
        var right = left
        while (iter.hasNext)
          right = iter.next()._2
        (left - k, right + k)
      }
    }).collect()

    val right_dup_rdd = right_rdd_with_indexed.flatMap(item => {
      var tmp_arr = mutable.ListBuffer[(Int, ((Double, Point), InternalRow))]()
      var part = 0
      if (right_rdd_bound.length < 128) {
        while (part < right_rdd_bound.length && right_rdd_bound(part)._1 <= item._2) {
          if (right_rdd_bound(part)._2 >= item._2) {
            tmp_arr += ((part, ((item._1._1, item._1._2._1), item._1._2._2.copy())))
          }
          part += 1
        }
      } else {
        part = binarySearch[(Long, Long)](right_rdd_bound, _._2.toDouble, item._2.toDouble)
        while (part < right_rdd_bound.length && right_rdd_bound(part)._1 <= item._2) {
          tmp_arr += ((part, ((item._1._1, item._1._2._1), item._1._2._2.copy())))
          part += 1
        }
      }
      tmp_arr
    })

    val right_dup_partitioned = MapDPartition(right_dup_rdd, right_rdd_bound.length)
      .mapPartitions(iter => iter.map(x => (x._2._1._1, (x._2._1._2, x._2._2))))

    left_partitioned_rdd.zipPartitions(right_dup_partitioned) { (leftIter, rightIter) => {
        val tmp_arr = mutable.ListBuffer[(InternalRow, Array[(InternalRow, Double)])]()
        val leftArr = leftIter.toArray
        val rightArr = rightIter.toArray
        for (i <- leftArr.indices) {
          var pos = 0
          if (rightArr.length < 128) {
            while (pos < rightArr.length - 1 && rightArr(pos)._1 <= leftArr(i)._1) pos += 1
          } else binarySearch[(Double, (Point, InternalRow))](rightArr, _._1, leftArr(i)._1)
          var tmp = Array[(InternalRow, Double)]()
          for (j <- (pos - k) until (pos + k))
            if (j >= 0 && j < rightArr.length) {
              tmp = tmp :+(rightArr(j)._2._2, leftArr(i)._2._1.minDist(rightArr(j)._2._1))
            }
          tmp_arr += (leftArr(i)._2._2 -> tmp.sortWith(_._2 < _._2).take(k))
        }
        tmp_arr.iterator
      }
    }
  }

  def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute().map(row =>
      (BindReferences.bindReference(left_key, left.output).eval(row)
        .asInstanceOf[Point], row)
    )

    val right_rdd = right.execute().map(row =>
      (BindReferences.bindReference(right_key, right.output).eval(row)
        .asInstanceOf[Point], row)
    )

    val dimension = right_rdd.first._1.coord.length
    val shift_vec = genRandomShiftVectors(dimension, num_shifts)

    var joined_rdd = zKNNPerIter(left_rdd, right_rdd, k, shift_vec(0))
    for (i <- 1 to num_shifts)
      joined_rdd = joined_rdd.union(zKNNPerIter(left_rdd, right_rdd, k, shift_vec(i)))

    joined_rdd.reduceByKey((left, right) =>
      (left ++ right).distinct.sortWith(_._2 < _._2).take(k), num_partition).flatMap(now => {
      val ans = mutable.ListBuffer[InternalRow]()
      now._2.foreach(x => ans += new JoinedRow(now._1, x._1))
      ans
    })
  }
}
