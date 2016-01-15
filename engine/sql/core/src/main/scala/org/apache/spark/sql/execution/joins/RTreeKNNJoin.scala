package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}

import org.apache.spark.sql.partitioner.{mapDPartition, strRangePartition}
import org.apache.spark.sql.spatial.{MBR, Dist, RTree, Point}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * Created by dong on 27-6-15.
 * RTree based kNN Join Implementation
 */
case class RTreeKNNJoin(leftKeys: Seq[Expression],
                        rightKeys: Seq[Expression],
                        kNN: Literal,
                        left: SparkPlan,
                        right: SparkPlan)
  extends BinaryNode {
  override def output = left.output ++ right.output

  final val num_shuffle_partitions = sqlContext.conf.numShufflePartitions
  final val sample_rate = sqlContext.conf.sampleRate
  final val max_entries_per_node = sqlContext.conf.maxEntriesPerNode
  final val theta_boost = sqlContext.conf.thetaBoost
  final val k = kNN.toString.toInt
  final val dimension = leftKeys.length

  override def execute() = {
    val leftRDD = left.execute().map(row => {
      val tmp : Array[Double] = new Array[Double](dimension)
      for (i <-0 to dimension - 1) {
        tmp(i) = BindReferences.bindReference(leftKeys(i), left.output).eval(row).asInstanceOf[Number].doubleValue()
      }
      (Point(tmp, dimension), row)
    })

    val rightRDD = right.execute().map(row => {
      val tmp : Array[Double] = new Array[Double](dimension)
      for (i <-0 to dimension - 1)
        tmp(i) = BindReferences.bindReference(rightKeys(i), right.output).eval(row).asInstanceOf[Number].doubleValue()
      (Point(tmp, dimension), row)
    })

    val (leftPartitioned, left_mbr_bound) = strRangePartition(leftRDD, dimension, num_shuffle_partitions, check_bound = true, sample_rate)

    val rightSampled = rightRDD.sample(withReplacement = false, sample_rate, System.currentTimeMillis).map(_._1).collect().zipWithIndex
    val right_rt = RTree(rightSampled, dimension, max_entries_per_node)

    val dim = new Array[Int](dimension)
    var remaining = theta_boost.toDouble
    for (i <- 0 to dimension - 1) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

//    val refined_mbr_bound = left_mbr_bound.flatMap(x => {
//      val low = x._1.low
//      val high = x._1.high
//      val step = new Array[Double](dimension)
//      for (i <- step.indices)
//        step(i) = (high.coord(i) - low.coord(i)) / dim(i)
//      def generate(state : Int, now: Array[Int], goal: Array[Int]) : Array[(MBR, Int)] = {
//        if (state == dimension) {
//          val cur_low = new Point(Array.fill[Double](dimension)(0))
//          val cur_high = new Point(Array.fill[Double](dimension)(0))
//          for (i <- 0 to dimension - 1) {
//            cur_low.coord(i) = low.coord(i) + step(i) * now(i)
//            cur_high.coord(i) = cur_low.coord(i) + step(i)
//          }
//          return Array[(MBR, Int)]((new MBR(cur_low, cur_high), x._2))
//        }
//        val ans = ListBuffer[(MBR, Int)]()
//        for (i <- 0 to goal(state) - 1) {
//          now(state) = i
//          ans ++= generate(state + 1, now, goal)
//        }
//        ans.toArray
//      }
//      generate(0, Array.fill[Int](dimension)(0), dim)
//    })

    val refined_mbr_bound = leftPartitioned.mapPartitionsWithIndex((id, iter) => {
      if (iter.hasNext) {
        val data = iter.map(_._1).toArray
        def recursiveGroupPoint(entries: Array[Point], cur_dim : Int, until_dim : Int) : Array[(Point, Double)] = {
          val len = entries.length.toDouble
          val grouped = entries.sortWith(_.coord(cur_dim) < _.coord(cur_dim)).grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
          if (cur_dim < until_dim) {
            grouped.map(now => {
              recursiveGroupPoint(now, cur_dim + 1, until_dim)
            }).flatMap(list => list)
          } else grouped.map(list => {
            val min = new Array[Double](dimension).map(x => Double.MaxValue)
            val max = new Array[Double](dimension).map(x => Double.MinValue)
            list.foreach(now => {
              for (i <- 0 to dimension - 1) min(i) = Math.min(min(i), now.coord(i))
              for (i <- 0 to dimension - 1) max(i) = Math.max(max(i), now.coord(i))
            })
            val mbr = new MBR(new Point(min), new Point(max))
            var cur_max = 0.0
            list.foreach(now => {
              val cur_dis = mbr.centroid.minDist(now)
              if (cur_dis > cur_max) cur_max = cur_dis
            })
            (mbr.centroid, cur_max)
          })
        }
        recursiveGroupPoint(data, 0, dimension - 1).map(x => (x._1, x._2 , id)).iterator
      } else Array().iterator
    }).collect()

    val theta = new Array[Double](refined_mbr_bound.length)
    for (i <- refined_mbr_bound.indices) {
      val query = refined_mbr_bound(i)._1
      val knn_mbr_ans = right_rt.kNN(query, k, keepSame = false)
      theta(i) = knn_mbr_ans(knn_mbr_ans.length - 1)._1.minDist(query) + (refined_mbr_bound(i)._2 * 2.0)
    }

    val bc_theta = sparkContext.broadcast(theta)

    val rightDuplicated = rightRDD.mapPartitions(iter => {
      iter.flatMap(x => {
        var list = ListBuffer[(Int, (Point, Row))]()
        val set = new mutable.HashSet[Int]
        for (i <- refined_mbr_bound.indices) {
          val pid = refined_mbr_bound(i)._3
          if (!set.contains(pid) && refined_mbr_bound(i)._1.minDist(x._1) < bc_theta.value(i)) {
            list += ((pid, x))
            set += pid
          }
        }
        list
      })
    })

    val rightDupPartitioned = mapDPartition(rightDuplicated, left_mbr_bound.length)
      .mapPartitions(iter => iter.map(_._2))

    leftPartitioned.zipPartitions(rightDupPartitioned) {
      (leftIter, rightIter) => {
        val ans = ListBuffer[Row]()
        val right_data = rightIter.toArray
        if (right_data.length > 0) {
          val right_index = RTree(right_data.map(_._1).zipWithIndex, dimension, max_entries_per_node)
          while (leftIter.hasNext) {
            val now = leftIter.next()
            val query = now._1
            val cur_ans = right_index.kNN(query, k, keepSame = false)
            cur_ans.foreach(x =>{
              ans += new JoinedRow(now._2, right_data(x._2)._2)
            })
          }
        }
        ans.iterator
      }
    }
  }
}
