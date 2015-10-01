package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.spatial._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, BinaryNode}

/**
 * Created by Dong Xie on 9/30/15.
 * Distance Join based on Cartesian Product
 */
case class CartesianDistanceJoin(
    left_keys: Seq[Expression],
    right_keys: Seq[Expression],
    l: Literal,
    left: SparkPlan,
    right: SparkPlan
  ) extends BinaryNode {
  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def output: Seq[Attribute] = left.output ++ right.output

  final val r = l.value.asInstanceOf[Number].doubleValue()

  override protected def doExecute(): RDD[InternalRow] =
    left.execute().cartesian(right.execute()).mapPartitions { iter =>
      val joinedRow = new JoinedRow
      iter.filter { row =>
        val point1 = left_keys.map(x => BindReferences.bindReference(x, right.output).eval(row._2)
          .asInstanceOf[Number].doubleValue).toArray
        val point2 = right_keys.map(x => BindReferences.bindReference(x, left.output).eval(row._1)
          .asInstanceOf[Number].doubleValue).toArray
        new Point(point1).minDist(new Point(point2)) <= r
      }.map(row => joinedRow(row._1, row._2))
    }
}
