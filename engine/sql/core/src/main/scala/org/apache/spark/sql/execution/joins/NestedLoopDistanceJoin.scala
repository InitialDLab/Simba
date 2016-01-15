package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.partitioner.mapDPartition
import org.apache.spark.sql.spatial.Point
import org.apache.spark.util.BoundedPriorityQueue

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Created by dong on 6/29/15.
 */
case class NestedLoopDistanceJoin(leftKeys: Seq[Expression],
                                  rightKeys: Seq[Expression],
                                  radius: Literal,
                                  left: SparkPlan,
                                  right: SparkPlan)
  extends BinaryNode {
  override def output = left.output ++ right.output

  final val num_shuffle_partitions = sqlContext.conf.numShufflePartitions
  final val r = radius.toString.toDouble
  final val dimension = leftKeys.length

  class DisOrdering extends Ordering[(Row, Double)] {
    override def compare(x : (Row, Double), y: (Row, Double)) = -x._2.compare(y._2)
  }

  override def execute() = {
    val leftRDD = left.execute().map(x => (x, 0))

    val rightRDD = right.execute().map(x => (x, 1))

    val totRDD = leftRDD.union(rightRDD)

    val totDupRDD = totRDD.flatMap(x => {
      val rand_no = new Random().nextInt(num_shuffle_partitions)
      var ans = List[(Int, (Int, Row))]()
      if (x._2 == 0) {
        val base = rand_no * num_shuffle_partitions
        for (i <- 0 to num_shuffle_partitions - 1)
          ans = ans :+ (base + i, (x._2, x._1.copy()))
      } else {
        for (i <- 0 to num_shuffle_partitions - 1)
          ans = ans :+ (i * num_shuffle_partitions + rand_no, (x._2, x._1.copy()))
      }
      ans
    })

    val totDupPartitioned = mapDPartition(totDupRDD, num_shuffle_partitions * num_shuffle_partitions)

    totDupPartitioned.mapPartitions(iter => {
      var leftData = ListBuffer[(Point, Row)]()
      var rightData = ListBuffer[(Point, Row)]()
      while (iter.hasNext) {
        val data = iter.next()
        if (data._2._1 == 0) {
          val tmp : Array[Double] = new Array[Double](dimension)
          for (i <-0 to dimension - 1) {
            tmp(i) = BindReferences.bindReference(leftKeys(i), left.output).eval(data._2._2).asInstanceOf[Number].doubleValue()
          }
          leftData += ((Point(tmp, dimension), data._2._2))
        } else {
          val tmp : Array[Double] = new Array[Double](dimension)
          for (i <-0 to dimension - 1)
            tmp(i) = BindReferences.bindReference(rightKeys(i), right.output).eval(data._2._2).asInstanceOf[Number].doubleValue()
          rightData += ((Point(tmp, dimension), data._2._2))
        }
      }

      val joined_ans = ListBuffer[Row]()

      leftData.foreach(x => rightData.foreach(y => {
        if (x._1.minDist(y._1) <= r)
          joined_ans += new JoinedRow(x._2, y._2)
      }))

      joined_ans.iterator
    })
  }
}
