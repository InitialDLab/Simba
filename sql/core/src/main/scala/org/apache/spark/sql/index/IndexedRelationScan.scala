package org.apache.spark.sql.index

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.spatial._

import scala.collection.mutable


/**
 * Created by Dong Xie on 9/29/15.
 * Physical Scan on Indexed Relation
 */

private[sql] class Interval(var min: (Double, Boolean), var max: (Double, Boolean)) extends Serializable {
  def this(min_val: Double = 0.0, max_val: Double = 0.0, left_closed: Boolean = true, right_closed: Boolean = true) {
    this((min_val, left_closed), (max_val, right_closed))
  }

  def isNull = min._1 > max._1 || (min._1 == max._1 && !(min._2 && max._2))

  def intersect(other: Interval): Interval = {
    val ans = new Interval()
    if (!other.isNull) {
      if (other.min._1 > max._1 || other.max._1 < min._1)
        ans.max = (ans.min._1 - 1, true)
      else {
        ans.min = if (min._1 < other.min._1) other.min else min
        ans.max = if (max._1 > other.max._1) other.max else max
      }
    } else ans.max = (ans.min._1 - 1, true)
    ans
  }

  override def toString = (if (min._2) "[" else "(") + min._1 + ", " + max._1 + (if (max._2) "]" else ")")
}

private[sql] case class IndexedRelationScan(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    relation: IndexedRelation)
  extends LeafNode with PredicateHelper {
  def getLeafInterval(x: Expression): (Interval, Attribute) = {
    x match {
      case EqualTo(left: NamedExpression, right: Literal) =>
        val tmp = right.value.asInstanceOf[Number].doubleValue()
        (new Interval(tmp, tmp), left.toAttribute)
      case LessThan(left: NamedExpression, right: Literal) =>
        (new Interval(Double.MinValue, right.value.asInstanceOf[Number].doubleValue(), false, false), left.toAttribute)
      case LessThanOrEqual(left: NamedExpression, right: Literal) =>
        (new Interval(Double.MinValue, right.value.asInstanceOf[Number].doubleValue(), false, true), left.toAttribute)
      case GreaterThan(left: NamedExpression, right: Literal) =>
        (new Interval(right.value.asInstanceOf[Number].doubleValue(), Double.MaxValue, false, false), left.toAttribute)
      case GreaterThanOrEqual(left: NamedExpression, right: Literal) =>
        (new Interval(right.value.asInstanceOf[Number].doubleValue(), Double.MaxValue, true, false), left.toAttribute)
      case _ =>
        null
    }
  }

  def conditionToInterval(condition: Expression, column: List[Attribute]): (Array[Interval], Array[Expression]) = {
    val leaf_nodes = splitCNFPredicates(condition)
    val intervals: Array[Interval] = new Array[Interval](column.length)
    for (i <- column.indices)
      intervals(i) = new Interval(Double.MinValue, Double.MaxValue, false, false)
    var ans = mutable.ArrayBuffer[Expression]()
    leaf_nodes.foreach {now =>
      val tmp_interval = getLeafInterval(now)
      if (tmp_interval != null) {
        for (i <- column.indices)
          if (column.indexOf(tmp_interval._2) == i)
            intervals(i) = intervals(i).intersect(tmp_interval._1)
      } else {
        now match {
          case InRange(point: Seq[NamedExpression], point_low: Seq[Expression], point_high: Seq[Expression]) =>
            for (i <- point.indices) {
              val id = column.indexOf(point(i).toAttribute)
              val low = point_low(i).asInstanceOf[Literal].toString.toDouble
              val high = point_high(i).asInstanceOf[Literal].toString.toDouble
              intervals(id) = intervals(id).intersect(new Interval(low, high))
            }
          case knn @ InKNN(point: Seq[NamedExpression], target: Seq[Expression], k: Literal) =>
            ans += knn
          case cr @ InCircleRange(point: Seq[NamedExpression], target: Seq[Expression], r: Literal) =>
            ans += cr
        }
      }
    }
    (intervals, ans.toArray)
  }

  override protected def doExecute(): RDD[InternalRow] = ???

  override def output: Seq[Attribute] = relation.output
}
