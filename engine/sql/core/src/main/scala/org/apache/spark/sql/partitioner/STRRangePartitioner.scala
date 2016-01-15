package org.apache.spark.sql.partitioner

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.spatial.{RTree, RTreeNode, MBR, Point}
import org.apache.spark.util.MutablePair
import org.apache.spark.{SparkConf, Partitioner, SparkEnv}
import org.apache.spark.sql.catalyst.expressions.Row

import scala.collection.mutable.ListBuffer

/**
 * Created by crystalove on 15-5-26.
 */
object strRangePartition extends DataPartition{
  def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply(originRDD: RDD[(Point, Row)], dimension: Int, est_partition: Int, check_bound: Boolean, sampleRate: Double)
  : (RDD[(Point, Row)], Array[(MBR, Int)]) = {
    val rdd = if (sortBasedShuffleOn) {
      originRDD.mapPartitions { iter => iter.map(row => {
        val cp = row
        (cp._1, cp._2.copy())
      })}
    } else {
      originRDD.mapPartitions { iter =>
        val mutablePair = new MutablePair[Point, Row]
        iter.map(row => mutablePair.update(row._1, row._2.copy()))
      }
    }

    val part = new STRRangePartitioner(est_partition, sampleRate, dimension, check_bound,  rdd)
    val shuffled = new ShuffledRDD[Point, Row, Row](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))

    (shuffled, part.mbrBound)
  }
}

class STRRangePartitioner(est_partition: Int,
                          sampleRate: Double,
                          dimension: Int,
                          check_bound: Boolean,
                          rdd: RDD[_ <: Product2[Point, Any]]) extends Partitioner {
  def numPartitions = partitions

  case class Bounds(min: Array[Double], max: Array[Double])

  val (mbrBound, partitions) = {
    val data_bounds = if (check_bound) {
      rdd.aggregate[Bounds](null)((bound, data) => {
        if (bound == null) {
          new Bounds(data._1.coord, data._1.coord)
        } else {
          new Bounds(bound.min.zip(data._1.coord).map(x => Math.min(x._1, x._2)),
            bound.max.zip(data._1.coord).map(x => Math.max(x._1, x._2)))
        }
      }, (left, right) => {
        if (left == null) right
        else if (right == null) left
        else {
          new Bounds(left.min.zip(right.min).map(x => Math.min(x._1, x._2)),
            left.max.zip(right.max).map(x => Math.max(x._1, x._2)))
        }
      })
    } else {
      new Bounds(Array.fill[Double](dimension)(Double.MinValue), Array.fill[Double](dimension)(Double.MaxValue))
    }


    //TODO The sample size should be tuned
    //val sampleSize = math.min(20.0 * est_partition, 1e6).toInt
    //val sampled = rdd.map(_._1).takeSample(withReplacement = false, sampleSize, System.currentTimeMillis)
    val sampled = rdd.sample(withReplacement = false, sampleRate, System.currentTimeMillis).map(_._1).collect()

    val dim = new Array[Int](dimension)
    var remaining = est_partition.toDouble
    for (i <- 0 to dimension - 1) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def recursiveGroupPoint(entries: Array[Point], now_min: Array[Double], now_max : Array[Double], cur_dim : Int, until_dim : Int) : Array[MBR] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(_.coord(cur_dim) < _.coord(cur_dim)).grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      val flag = 1 << cur_dim
      if (cur_dim < until_dim) {
        var ans = ListBuffer[MBR]()
        for (i <- grouped.indices) {
          val cur_min = now_min
          val cur_max = now_max
          if (i == 0 && i == grouped.length - 1) {
            cur_min(cur_dim) = data_bounds.min(cur_dim)
            cur_max(cur_dim) = data_bounds.max(cur_dim)
          } else if (i == 0) {
            cur_min(cur_dim) = data_bounds.min(cur_dim)
            cur_max(cur_dim) = grouped(i + 1).head.coord(cur_dim)
          } else if (i == grouped.length - 1) {
            cur_min(cur_dim) = grouped(i).head.coord(cur_dim)
            cur_max(cur_dim) = data_bounds.max(cur_dim)
          } else {
            cur_min(cur_dim) = grouped(i).head.coord(cur_dim)
            cur_max(cur_dim) = grouped(i + 1).head.coord(cur_dim)
          }
          ans ++= recursiveGroupPoint(grouped(i), cur_min, cur_max, cur_dim + 1, until_dim)
        }
        ans.toArray
      } else {
        var ans = ListBuffer[MBR]()
        for (i <- grouped.indices) {
          if (i == 0 && i == grouped.length - 1) {
            now_min(cur_dim) = data_bounds.min(cur_dim)
            now_max(cur_dim) = data_bounds.max(cur_dim)
          } else if (i == 0) {
            now_min(cur_dim) = data_bounds.min(cur_dim)
            now_max(cur_dim) = grouped(i + 1).head.coord(cur_dim)
          } else if (i == grouped.length - 1) {
            now_min(cur_dim) = grouped(i).head.coord(cur_dim)
            now_max(cur_dim) = data_bounds.max(cur_dim)
          } else {
            now_min(cur_dim) = grouped(i).head.coord(cur_dim)
            now_max(cur_dim) = grouped(i + 1).head.coord(cur_dim)
          }
          ans += new MBR(new Point(now_min.clone()), new Point(now_max.clone()))
        }
        ans.toArray
      }
    }

    val cur_min = new Array[Double](dimension)
    val cur_max = new Array[Double](dimension)
    val mbrs = recursiveGroupPoint(sampled, cur_min, cur_max, 0, dimension - 1)

    (mbrs.zipWithIndex, mbrs.length)
  }

  val rt = RTree.buildFromMBR(mbrBound.map(x => (x._1, x._2, 1)), 25)

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Point]

    rt.circleRangeMBR(k, 0.0).head._2
  }
}
