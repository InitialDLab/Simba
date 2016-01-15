package org.apache.spark.sql.execution.joins

/**
 * Created by crystalove on 15-5-20.
 */

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.partitioner.{RangePartition, determinRangePartition, mapDPartition, RangePartitioner}
import org.apache.spark.sql.spatial.{Point, ZValue}
import scala.collection.mutable.ListBuffer
import scala.util.Random
import org.apache.spark.rdd.RDD

case class zKNNJoin(leftKeys: Seq[Expression],
                    rightKeys: Seq[Expression],
                    kNN: Literal,
                    left: SparkPlan,
                    right: SparkPlan)
  extends BinaryNode {
  override def output = left.output ++ right.output

  def k = kNN.toString.toInt
  //Parameters that set in sqlContext.conf
  def numPartitions = sqlContext.conf.numShufflePartitions
  def numShifts = sqlContext.conf.zknnShifts
  def dimension = leftKeys.length
  def shiftVec = genRandomShiftVectors(dimension, numShifts)

  def genRandomShiftVectors(dimension : Int, shift : Int): Array[Array[Int]] = {
    val r = new Random(System.currentTimeMillis)
    val ans = Array.ofDim[Int](shift + 1, dimension)
    for (i <- 0 to shift)
      for (j <- 0 to dimension - 1) {
        if (i == 0) ans(i)(j) = 0
        else ans(i)(j) = Math.abs(r.nextInt(100))
      }
    ans
  }

  def calcShiftArray(point : Point, shift : Array[Int]) : Array[Int] = {
    val len = point.dimensions
    val ans = new Array[Int](len)

    for (i <- 0 to len - 1)
      ans(i) = point.coord(i).toInt + shift(i)
    ans
  }

  def binarySearch[T](array: Array[T], func: T => Double, key: Double): Int = {
    var left = 0
    var right = array.length - 1
    while (left < right) {
      val mid = (left + right) >> 1
      if (func(array(mid)) <= key)
        left = mid + 1
      else right = mid
    }
    left
  }

  def zKNNPerIteration(leftRDD: RDD[(Point, Row)], rightRDD: RDD[(Point, Row)], k: Int, shift: Array[Int])
  : RDD[(Row, Array[(Row, Double)])] = {
    val packedLeftRDD = leftRDD.map(row =>
      (ZValue(calcShiftArray(row._1, shift)).toDouble, row))

    val (partitionedLeftRDD, leftRDDBound) = RangePartition(packedLeftRDD, numPartitions)

    val packedRightRDD = rightRDD.mapPartitions(iter => iter.map(row =>
      (ZValue(calcShiftArray(row._1, shift)).toDouble, row)))
    val partitionedRightRDD = determinRangePartition(packedRightRDD, leftRDDBound)


    val indexedRightRDD = partitionedRightRDD.zipWithIndex()
    val rightRDDBound = indexedRightRDD.mapPartitions(iter => new Iterator[(Long, Long)] {
      def hasNext = iter.hasNext
      def next() = {
        val left = iter.next()._2
        var right = left
        while (iter.hasNext)
          right = iter.next()._2
        (left - k, right + k)
      }
    }).collect()

    val duplicatedRightRDD = indexedRightRDD.flatMap(item => {
      var tmp_arr: List[(Int, ((Double, Point), Row))] = List()
      var part = 0
      if (rightRDDBound.length < 128)
        while (part < rightRDDBound.length && rightRDDBound(part)._1 <= item._2) {
          if (rightRDDBound(part)._2 >= item._2) tmp_arr = tmp_arr :+ (part, ((item._1._1, item._1._2._1), item._1._2._2.copy()))
          part += 1
        }
      else {
        part = binarySearch[(Long, Long)](rightRDDBound, _._2.toDouble, item._2.toDouble)
        while (part < rightRDDBound.length && rightRDDBound(part)._1 <= item._2) {
          tmp_arr = tmp_arr :+ (part, ((item._1._1, item._1._2._1), item._1._2._2.copy()))
          part += 1
        }
      }
      tmp_arr
    })

    val partitionDupRightRDD = mapDPartition(duplicatedRightRDD, rightRDDBound.length)
      .mapPartitions(iter => iter.map(x => (x._2._1._1, (x._2._1._2, x._2._2))))

    partitionedLeftRDD.zipPartitions(partitionDupRightRDD) {
      (leftIter, rightIter) =>  {
        val tmp_arr =  ListBuffer[(Row, Array[(Row, Double)])]()
        val leftArr = leftIter.toArray
        val rightArr = rightIter.toArray
        for (i <- 0 to leftArr.length - 1) {
          var pos = 0
          if (rightArr.length < 128)
            while (pos < rightArr.length - 1 && rightArr(pos)._1 <= leftArr(i)._1)
              pos += 1
          else binarySearch[(Double, (Point, Row))](rightArr, _._1, leftArr(i)._1)
          var tmp = Array[(Row, Double)]()
          for (j <- (pos - k) to (pos + k - 1))
            if (j >= 0 && j < rightArr.length)
              tmp = tmp :+ (rightArr(j)._2._2, leftArr(i)._2._1.minDist(rightArr(j)._2._1))
          tmp_arr += (leftArr(i)._2._2 -> tmp.sortWith(_._2 < _._2).take(k))
        }
        tmp_arr.iterator
      }
    }
  }

  def execute() = {
    val leftRDD = left.execute().map(row => {
      val tmp : Array[Int] = new Array[Int](dimension)
      for (i <-0 to dimension - 1)
        tmp(i) = BindReferences.bindReference(leftKeys(i), left.output).eval(row).asInstanceOf[Number].intValue()
      (Point(tmp.map(x => x.toDouble), dimension), row)
    })
    val rightRDD = right.execute().map(row => {
      val tmp : Array[Int] = new Array[Int](dimension)
      for (i <-0 to dimension - 1)
        tmp(i) = BindReferences.bindReference(rightKeys(i), right.output).eval(row).asInstanceOf[Number].intValue()
      (Point(tmp.map(x => x.toDouble), dimension), row)
    })

    var joinedRDD = zKNNPerIteration(leftRDD, rightRDD, k, shiftVec(0))
    for (i <- 1 to numShifts)
      joinedRDD = joinedRDD.union(zKNNPerIteration(leftRDD, rightRDD, k, shiftVec(i)))

    joinedRDD.reduceByKey((left, right) =>
      (left ++ right).distinct.sortWith(_._2 < _._2).take(k), numPartitions).flatMap(now => {
      val ans = ListBuffer[Row]()
      now._2.foreach(x => ans += new JoinedRow(now._1, x._1))
      ans
    })
  }
}
