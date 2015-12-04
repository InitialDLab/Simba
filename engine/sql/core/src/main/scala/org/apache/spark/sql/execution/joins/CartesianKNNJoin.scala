package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.spatial._
import org.apache.spark.sql.execution.{SparkPlan, BinaryNode}

/**
 * Created by Dong Xie on 9/30/15.
 * KNN Join based on Cartesian Product
 */
case class CartesianKNNJoin(
    left_keys: Seq[Expression],
    right_keys: Seq[Expression],
    l: Literal,
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {
  override def outputPartitioning = left.outputPartitioning

  override def output = left.output ++ right.output

  final val k = l.value.asInstanceOf[Number].intValue()

  override protected def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute()
    val right_rdd = right.execute()

    left_rdd.map(row =>
      (new Point(left_keys.map(x => BindReferences.bindReference(x, left.output).eval(row)
        .asInstanceOf[Number].doubleValue()).toArray), row)
    ).cartesian(right_rdd).map {
      case (l: (Point, InternalRow), r: InternalRow) =>
        val tmp_point = new Point(right_keys.map(x => BindReferences.bindReference(x, right.output).eval(r)
          .asInstanceOf[Number].doubleValue()).toArray)
        l._2 -> List((tmp_point.minDist(l._1), r))
    }.reduceByKey {
      case (l_list: Seq[(Double, InternalRow)], r_list: Seq[(Double, InternalRow)]) =>
        (l_list ++ r_list).sortWith(_._1 < _._1).take(k)
    }.flatMapValues(list => list).mapPartitions {iter =>
      val joinedRow = new JoinedRow
      iter.map(r => joinedRow(r._1, r._2._2))
    }
  }
}
