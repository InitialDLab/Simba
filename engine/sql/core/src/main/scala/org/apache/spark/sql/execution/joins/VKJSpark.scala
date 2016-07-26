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
import org.apache.spark.sql.partitioner.{MapDPartition, VoronoiPartition}
import org.apache.spark.sql.spatial.Point
import org.apache.spark.util.BoundedPriorityQueue

import scala.collection.mutable
import scala.util.Random
import scala.util.control.Breaks

/**
  * Created by dong on 1/20/16.
  * KNN Join based on Voronoi Partitioning
  */
case class VKJSpark(left_key: Expression,
                    right_key: Expression,
                    kNN: Literal,
                    left: SparkPlan,
                    right: SparkPlan)
  extends BinaryNode{
  override def output: Seq[Attribute] = left.output ++ right.output

  // Parameters that set in sqlContext.conf
  final val num_partitions = sqlContext.conf.numShufflePartitions
  final val num_of_pivot_sets = sqlContext.conf.voronoiPivotSetSize
  final val num_of_pivots = num_partitions * sqlContext.conf.thetaBoost
  final val k = kNN.toString.toInt

  case class LeftMetaInfo(pid: Int, upperBound: Double, var theta : Double = Double.MaxValue)
  case class RightMetaInfo(pid: Int, upperBound: Double, knnOfPivot: Array[Double])

  private def generatePivots(origin: RDD[Point], num_pivots : Int) = {
    val sampled = origin.takeSample(withReplacement = false,
      num_of_pivot_sets * num_pivots, System.currentTimeMillis())
    val shuffled = Random.shuffle(sampled.toList).toArray
    var best = 0.0
    var best_offset = -1
    for (offset <- 0.to(shuffled.length - 1, num_of_pivots)) {
      var sum = 0.0
      for (i <- offset until (offset + num_of_pivots))
        for (j <- (i + 1) until (offset + num_of_pivots))
          sum += shuffled(i).minDist(shuffled(j))
      if (best < sum) {
        best = sum
        best_offset = offset
      }
    }
    shuffled.slice(best_offset, best_offset + num_of_pivots)
  }

  private def geoGrouping(pivots: Array[Point], cell_size: Array[Int], num_group: Int) = {
    assert(pivots.length == cell_size.length)
    assert(pivots.length >= num_group)
    val remaining = new mutable.HashSet[Int]
    for (i <- pivots.indices) remaining += i
    var best = 0.0
    var best_id = -1
    remaining.foreach(x => {
      var tmp = 0.0
      for (j <- pivots.indices)
        tmp += pivots(x).minDist(pivots(j))
      if (tmp > best) {
        best = tmp
        best_id = x
      }
    })

    val group_base = new Array[Int](num_group)
    class SizeOrdering extends Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = - x._2.compare(y._2)
    }
    val group_size = new mutable.PriorityQueue[(Int, Int)]()(new SizeOrdering)
    val grouping = new Array[Array[Int]](num_group)
    val pivot_to_group = new Array[Int](pivots.length)
    group_base(0) = best_id
    remaining -= best_id

    for (i <- 1 until num_group) {
      var best = 0.0
      var best_id = -1
      remaining.foreach(x => {
        var tmp = 0.0
        for (j <- 0 until i)
          tmp += pivots(x).minDist(pivots(group_base(j)))
        if (tmp > best) {
          best = tmp
          best_id = x
        }
      })
      group_base(i) = best_id
      remaining -= best_id
    }

    for (i <- group_base.indices) {
      group_size += ((i, cell_size(group_base(i))))
      grouping(i) = Array(group_base(i))
      pivot_to_group(group_base(i)) = i
    }

    while (remaining.nonEmpty) {
      val now = group_size.dequeue()
      var best = Double.MaxValue
      var best_id = -1
      remaining.foreach(x => {
        var tmp = 0.0
        grouping(now._1).foreach(p => tmp += pivots(p).minDist(pivots(x)))
        if (tmp < best) {
          best = tmp
          best_id = x
        }
      })
      grouping(now._1) = grouping(now._1) :+ best_id
      pivot_to_group(best_id) = now._1
      group_size += ((now._1, now._2 + cell_size(best_id)))
      remaining -= best_id
    }

    (grouping, pivot_to_group)
  }

  private def knnObjectForSinglePoint(left_item: (Point, InternalRow),
                              left_pid: Int,
                              sort_by_dist: IndexedSeq[(Int, Double)],
                              pivots: Array[Point],
                              right_meta_info: Array[RightMetaInfo],
                              right_data_by_pivot: Array[Array[(Point, InternalRow, Double)]])
  : Iterable[InternalRow] = {
    val left_point = left_item._1
    val left_row = left_item._2

    val dist_to_pivot: Array[Double] = new Array[Double](num_of_pivots)

    for (i <- dist_to_pivot.indices)
      dist_to_pivot(i) = left_point.minDist(pivots(i))

    val theta_pq = new BoundedPriorityQueue[Double](k)(new DescendOrdering)
    right_meta_info.foreach(right => {
      if (right_data_by_pivot(right.pid) != null){
        right.knnOfPivot.foreach {d =>
          val tempDist = dist_to_pivot(right.pid) + d
          theta_pq += tempDist
        }
      }
    })
    var theta = theta_pq.head

    var knn_of_left_item =
      new BoundedPriorityQueue[(InternalRow, Double)](k)(new DescendOrderRowDistance)
    val outer_loop = new Breaks
    outer_loop.breakable {
      sort_by_dist.foreach(cur_part => {
        if (cur_part._2 == Double.MaxValue) outer_loop.break()

        val leftObjectToHP = if (cur_part._1 == left_pid) 0.0
        else (dist_to_pivot(cur_part._1) * dist_to_pivot(cur_part._1) -
          dist_to_pivot(left_pid) * dist_to_pivot(left_pid)) /
          (2.0 * pivots(left_pid).minDist(pivots(cur_part._1)))

        if (leftObjectToHP <= theta) {
          val loop = new Breaks
          loop.breakable{
            for (i <- right_data_by_pivot(cur_part._1).indices) {
              val rightItem = right_data_by_pivot(cur_part._1)(i)
              if (dist_to_pivot(cur_part._1) > rightItem._3 + theta) loop.break()

              knn_of_left_item += ((rightItem._2, rightItem._1.minDist(left_point)))
              if (knn_of_left_item.size == k) theta = knn_of_left_item.head._2
            }
          }
        }
      })
    }
    knn_of_left_item.map(item => new JoinedRow(left_row, item._1))
  }


  private def knnObjectForEachPartition(left_data_by_pivot: Array[Array[(Point, InternalRow)]],
                                right_data_by_pivot: Array[Array[(Point, InternalRow, Double)]],
                                pivots: Array[Point],
                                right_meta_info: Array[RightMetaInfo])
  : mutable.ListBuffer[InternalRow] = {
    var res = mutable.ListBuffer[InternalRow]()

    for (i <- right_data_by_pivot.indices)
      if (right_data_by_pivot(i) != null) {
        right_data_by_pivot(i) = right_data_by_pivot(i).sortWith((x, y) => x._3 > y._3)
      }

    for (left_pid <- left_data_by_pivot.indices) { // foreach left partition
      if (left_data_by_pivot(left_pid) != null) {
        val leftData = left_data_by_pivot(left_pid)
        val sortByDist = (0 until num_of_pivots).map(x => {
          if (right_data_by_pivot(x) == null) (x, Double.MaxValue)
          else (x, pivots(left_pid).minDist(pivots(x)))
        }).sortWith(_._2 < _._2)

        leftData.foreach(leftItem =>
          res ++= knnObjectForSinglePoint(leftItem, left_pid, sortByDist,
                    pivots, right_meta_info, right_data_by_pivot)
        )
      }
    }
    res
  }

  class DescendOrdering extends Ordering[Double] {
    override def compare(x: Double, y: Double): Int = -x.compare(y)
  }

  class DescendOrderRowDistance extends Ordering[(InternalRow, Double)] {
    def compare(a: (InternalRow, Double), b: (InternalRow, Double)): Int = {
      - a._2.compare(b._2)
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute().map(row =>
      (BindReferences.bindReference(left_key, left.output).eval(row)
        .asInstanceOf[Point], row)
    )

    val right_rdd = right.execute().map(row =>
      (BindReferences.bindReference(right_key, right.output).eval(row)
        .asInstanceOf[Point], row)
    )

    val pivots = generatePivots(left_rdd.map(_._1).union(right_rdd.map(_._1)), num_of_pivots)
    val bc_pivots = sparkContext.broadcast(pivots)
    val left_with_pivots = left_rdd.mapPartitions(iter => iter.map(x => {
      var nearestDist = Double.MaxValue
      var ans = -1
      val point = x._1
      val local_pivots = bc_pivots.value
      for (i <- local_pivots.indices){
        val dist = point.minDist(local_pivots(i))
        if (dist < nearestDist) {
          nearestDist = dist
          ans = i
        }
      }
      (ans, x)
    }))
    val right_with_pivots = right_rdd.mapPartitions(iter => iter.map(x => {
      var nearestDist = Double.MaxValue
      var ans = -1
      val point = x._1
      val local_pivots = bc_pivots.value
      for (i <- local_pivots.indices){
        val dist = point.minDist(local_pivots(i))
        if (dist < nearestDist) {
          nearestDist = dist
          ans = i
        }
      }
      (ans, x)
    }))

    // calculate the number of records in every partition of the left table
    val cell_size = left_with_pivots.aggregate(Array.fill[Int](num_of_pivots)(0))((tmp, now) => {
      tmp(now._1) += 1
      tmp
    }, (left, right) => {
      left.zip(right).map(x => x._1 + x._2)
    })

    val (grouping, pivot_to_group) = geoGrouping(pivots, cell_size, num_partitions)

    val left_partitioned = VoronoiPartition(left_with_pivots, pivot_to_group, num_partitions)
    val right_partitioned = VoronoiPartition(right_with_pivots, pivot_to_group, num_partitions)

    val tmp_left_meta_info = left_partitioned.mapPartitions(iter => {
      val data = iter.toArray
      data.groupBy(_._1).map(x => {
        val pivot = bc_pivots.value(x._1)
        var upperBound = Double.MinValue
        x._2.foreach(p => {
          val dist = p._2._1.minDist(pivot)
          if (dist > upperBound) upperBound = dist
        })
        LeftMetaInfo(x._1, upperBound)
      }).iterator
    }).collect()

    val right_meta_info = right_partitioned.mapPartitions(iter => {
      val data = iter.toArray
      data.groupBy(_._1).map(x => {
        var upperBound = Double.MinValue
        val pivot = bc_pivots.value(x._1)
        val minDist = new BoundedPriorityQueue[Double](k)(new DescendOrdering)
        x._2.foreach(p => {
          val dist = p._2._1.minDist(pivot)
          if (dist > upperBound) upperBound = dist
          minDist += dist
        })
        RightMetaInfo(x._1, upperBound, minDist.toArray)
      }).iterator
    }).collect()

    val left_meta_info = new mutable.HashMap[Int, LeftMetaInfo]
    tmp_left_meta_info.foreach(left => {
      val thetaArray = new BoundedPriorityQueue[Double](k)(new DescendOrdering)
      right_meta_info.foreach(right => {
        right.knnOfPivot.foreach {d =>
          val leftPivot = pivots(left.pid)
          val rightPivot = pivots(right.pid)
          val tempDist = left.upperBound + leftPivot.minDist(rightPivot) + d
          thetaArray += tempDist
        }
      })
      left.theta = thetaArray.head
      left_meta_info(left.pid) = left
    })

    class PartitionOrdering extends Ordering[LeftMetaInfo] {
      override def compare(x: LeftMetaInfo, y: LeftMetaInfo): Int =
        x.pid.compare(y.pid)
    }

    val lower_bounds: Array[Array[Double]] = new Array(num_partitions)

    for (i <- grouping.indices) {
      lower_bounds(i) = new Array[Double](num_of_pivots)
      right_meta_info.foreach(right => {
        var minLB = right.upperBound + 1
        grouping(i).foreach(x => {
          if (left_meta_info.get(x).isDefined) {
            val left = left_meta_info(x)
            val lb = pivots(left.pid).minDist(pivots(right.pid)) - left.upperBound - left.theta
            if (lb < minLB) minLB = lb
          }
        })
        lower_bounds(i)(right.pid) = minLB
      })
    }

    val right_dup = right_partitioned.mapPartitions{iter => {
      var ans = mutable.ListBuffer[(Int, ((Point, Int), InternalRow))]()
      while (iter.hasNext) {
        val now = iter.next()
        for (left <- 0 until num_partitions)
          if (now._2._1.minDist(bc_pivots.value(now._1)) >= lower_bounds(left)(now._1)) {
            ans += ((left, ((now._2._1, now._1), now._2._2)))
          }
      }
      ans.toArray.iterator
    }}

    val right_dup_partitioned = MapDPartition(right_dup, num_partitions)

    left_partitioned.zipPartitions(right_dup_partitioned) {
      (leftIter, rightIter) => {
        var ans = mutable.ListBuffer[InternalRow]()
        if (rightIter.hasNext) {
          val leftDataByPivot = new Array[Array[(Point, InternalRow)]](num_of_pivots)

          leftIter.foreach(x => {
            if (leftDataByPivot(x._1) == null) leftDataByPivot(x._1) = Array(x._2)
            else leftDataByPivot(x._1) = leftDataByPivot(x._1) :+ x._2
          })

          val rightDataByPivot = new Array[Array[(Point, InternalRow, Double)]](num_of_pivots)

          // x._1: where to shuffle, x._2._1._2: the partitionId of right element
          rightIter.foreach(x => {
            if (rightDataByPivot(x._2._1._2) == null) {
              rightDataByPivot(x._2._1._2) = Array((x._2._1._1, x._2._2,
                bc_pivots.value(x._2._1._2).minDist(x._2._1._1)))
            } else {
              rightDataByPivot(x._2._1._2) = rightDataByPivot(x._2._1._2) :+
                (x._2._1._1, x._2._2, bc_pivots.value(x._2._1._2).minDist(x._2._1._1))
            }
          })
          ans ++= knnObjectForEachPartition(leftDataByPivot, rightDataByPivot,
                    bc_pivots.value, right_meta_info)
        }
        ans.iterator
      }
    }
  }
}
