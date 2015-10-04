package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.execution.{SparkPlan, BinaryNode}
import org.apache.spark.sql.partitioner.MapDPartition
import org.apache.spark.sql.spatial._

import scala.util.Random
import scala.collection.mutable

/**
 * Created by Dong Xie on 9/30/15.
 * Distance Join based on Nested Loop Approach
 */
case class NestedLoopDistanceJoin(
    left_keys: Seq[Expression],
    right_keys: Seq[Expression],
    l: Literal,
    left: SparkPlan,
    right: SparkPlan
  ) extends BinaryNode {
  override def output = left.output ++ right.output

  final val num_partitions = sqlContext.conf.numShufflePartitions
  final val r = NumberConverter.literalToDouble(l)
  final val dimension = left_keys.length

  override protected def doExecute(): RDD[InternalRow] = {
    val tot_rdd = left.execute().map((0, _)).union(right.execute().map((1, _)))

    val tot_dup_rdd = tot_rdd.flatMap {x =>
      val rand_no = new Random().nextInt(num_partitions)
      var ans = mutable.ListBuffer[(Int, (Int, InternalRow))]()
      if (x._1 == 0) {
        val base = rand_no * num_partitions
        for (i <- 0 to num_partitions - 1)
          ans += ((base + i, x))
      } else {
        for (i <- 0 to num_partitions - 1)
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
          val tmp_point = new Point (left_keys.map(x => BindReferences.bindReference(x, left.output).eval(data._2._2)
            .asInstanceOf[Number].doubleValue()).toArray)
          left_data += ((tmp_point, data._2._2))
        } else {
          val tmp_point = new Point (right_keys.map(x => BindReferences.bindReference(x, right.output).eval(data._2._2)
            .asInstanceOf[Number].doubleValue()).toArray)
          right_data += ((tmp_point, data._2._2))
        }
      }

      val joined_ans = mutable.ListBuffer[InternalRow]()

      left_data.foreach {left =>
        right_data.foreach {right =>
          if (left._1.minDist(right._1) <= r)
            joined_ans += new JoinedRow(left._2, right._2)
        }
      }

      joined_ans.iterator
    }
  }
}
