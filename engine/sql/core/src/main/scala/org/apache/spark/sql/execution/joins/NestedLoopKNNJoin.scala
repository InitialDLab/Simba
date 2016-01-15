package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.partitioner.{mapDPartition, strRangePartition}
import org.apache.spark.sql.spatial.{Dist, RTree, Point}
import org.apache.spark.util.BoundedPriorityQueue

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Created by Dong Xie on 6/27/2015.
 * Brute Force Nested Loop KNN Join
 */
case class NestedLoopKNNJoin(leftKeys: Seq[Expression],
                             rightKeys: Seq[Expression],
                             kNN: Literal,
                             left: SparkPlan,
                             right: SparkPlan)
  extends BinaryNode {
  override def output = left.output ++ right.output

  final val num_shuffle_partitions = sqlContext.conf.numShufflePartitions
  final val k = kNN.toString.toInt
  final val dimension = leftKeys.length

  class DisOrdering extends Ordering[(Row, Double)] {
    override def compare(x : (Row, Double), y: (Row, Double)) = -x._2.compare(y._2)
  }

  override def execute() = {
    val leftRDD = left.execute().map(row => {
      val tmp : Array[Double] = new Array[Double](dimension)
      for (i <-0 to dimension - 1) {
        tmp(i) = BindReferences.bindReference(leftKeys(i), left.output).eval(row).asInstanceOf[Number].doubleValue()
      }
      (Point(tmp, dimension), row)
    }).map(x => (x, 0))
    val rightRDD = right.execute().map(row => {
      val tmp : Array[Double] = new Array[Double](dimension)
      for (i <-0 to dimension - 1)
        tmp(i) = BindReferences.bindReference(rightKeys(i), right.output).eval(row).asInstanceOf[Number].doubleValue()
      (Point(tmp, dimension), row)
    }).map(x => (x, 1))

    val totRDD = leftRDD.union(rightRDD)

    val totDupRDD = totRDD.flatMap(x => {
      val rand_no = new Random().nextInt(num_shuffle_partitions)
      var ans = List[(Int, ((Point, Int), Row))]()
      if (x._2 == 0) {
        val base = rand_no * num_shuffle_partitions
        for (i <- 0 to num_shuffle_partitions - 1)
          ans = ans :+ (base + i, ((x._1._1, x._2), x._1._2.copy()))
      } else {
        for (i <- 0 to num_shuffle_partitions - 1)
          ans = ans :+ (i * num_shuffle_partitions + rand_no, ((x._1._1, x._2), x._1._2.copy()))
      }
      ans
    })

    val totDupPartitioned = mapDPartition(totDupRDD, num_shuffle_partitions * num_shuffle_partitions)

    totDupPartitioned.mapPartitions(iter => {
      var leftData = ListBuffer[(Point, Row)]()
      var rightData = ListBuffer[(Point, Row)]()
      while (iter.hasNext) {
        val data = iter.next()
        if (data._2._1._2 == 0) leftData += ((data._2._1._1, data._2._2))
        else rightData += ((data._2._1._1, data._2._2))
      }

      val joined_ans = ListBuffer[(Row, Array[(Row, Double)])]()

      leftData.foreach(x => {
        var pq = new BoundedPriorityQueue[(Row, Double)](k)(new DisOrdering)
        rightData.foreach(cur => pq += ((cur._2, cur._1.minDist(x._1))))
        joined_ans += (x._2 -> pq.toArray)
      })
      joined_ans.iterator
    }).reduceByKey((left, right) =>
      (left ++ right).sortWith(_._2 < _._2).take(k)
    , num_shuffle_partitions).flatMap(now => {
      val ans = ListBuffer[Row]()
      now._2.foreach(x => ans += new JoinedRow(now._1, x._1))
      ans
    })
  }
}
