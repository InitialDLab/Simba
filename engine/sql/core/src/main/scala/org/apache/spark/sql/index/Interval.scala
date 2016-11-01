/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.spark.sql.index

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.spatial.Point

import scala.collection.mutable

/**
  * Created by gefei on 16-7-6.
  */
private[sql] class Interval(var min: (Double, Boolean),
                            var max: (Double, Boolean)) extends Serializable with PredicateHelper{
  def this(min_val: Double = 0.0, max_val: Double = 0.0,
           left_closed: Boolean = true, right_closed: Boolean = true) {
    this((min_val, left_closed), (max_val, right_closed))
  }

  def isNull: Boolean = min._1 > max._1 || (min._1 == max._1 && !(min._2 && max._2))

  def intersect(other: Interval): Interval = {
    val ans = new Interval()
    if (!other.isNull) {
      if (other.min._1 > max._1 || other.max._1 < min._1) {
        ans.max = (ans.min._1 - 1, true)
      } else {
        ans.min = if (min._1 < other.min._1) other.min else min
        ans.max = if (max._1 > other.max._1) other.max else max
      }
    } else ans.max = (ans.min._1 - 1, true)
    ans
  }

  override def toString: String =
    (if (min._2) "[" else "(") + min._1 + ", " + max._1 + (if (max._2) "]" else ")")
}

object Interval extends PredicateHelper{
  def getLeafInterval(x: Expression): (Interval, Attribute) = {
    x match {
      case EqualTo(left: NamedExpression, right: Literal) =>
        val tmp = NumberConverter.literalToDouble(right)
        (new Interval(tmp, tmp), left.toAttribute)
      case LessThan(left: NamedExpression, right: Literal) =>
        (new Interval(Double.MinValue, NumberConverter.literalToDouble(right),
          left_closed = false, right_closed = false), left.toAttribute)
      case LessThanOrEqual(left: NamedExpression, right: Literal) =>
        (new Interval(Double.MinValue, NumberConverter.literalToDouble(right),
          left_closed = false, right_closed = true), left.toAttribute)
      case GreaterThan(left: NamedExpression, right: Literal) =>
        (new Interval(NumberConverter.literalToDouble(right), Double.MaxValue,
          left_closed = false, right_closed = false), left.toAttribute)
      case GreaterThanOrEqual(left: NamedExpression, right: Literal) =>
        (new Interval(NumberConverter.literalToDouble(right), Double.MaxValue,
          left_closed = true, right_closed = false), left.toAttribute)
      case _ =>
        null
    }
  }
  def conditionToInterval(condition: Expression, column: List[Attribute], dimension: Int)
  : (Array[Interval], Array[Expression], Boolean) = {
    var all_knn_flag = true
    val leaf_nodes = splitConjunctivePredicates(condition) // split AND expression
    val intervals: Array[Interval] = new Array[Interval](dimension)
    for (i <- 0 until dimension)
      intervals(i) = new Interval(Double.MinValue, Double.MaxValue, false, false)
    var ans = mutable.ArrayBuffer[Expression]()
    leaf_nodes.foreach {now =>
      val tmp_interval = getLeafInterval(now)
      if (tmp_interval != null) {
        for (i <- 0 until dimension)
          if (column.indexOf(tmp_interval._2) == i) {
            intervals(i) = intervals(i).intersect(tmp_interval._1)
          }
          all_knn_flag = false
      } else {
        now match {
          case range @ InRange(point: Expression, point_low, point_high) =>
            val low = point_low.asInstanceOf[Literal].value.asInstanceOf[Point].coord
            val high = point_high.asInstanceOf[Literal].value.asInstanceOf[Point].coord
            for (i <- 0 until dimension) {
              intervals(i) = intervals(i).intersect(new Interval(low(i), high(i)))
            }
            all_knn_flag = false
          case knn @ InKNN(point: Expression, target, k: Literal) =>
            ans += knn
          case cr @ InCircleRange(point: Expression, target, r: Literal) =>
            ans += cr
            all_knn_flag = false
          case _ =>
            all_knn_flag = false
        }
      }
    }
    (intervals, ans.toArray, all_knn_flag)
  }

  def getBoundNumberForInterval(interval: Interval,
        range_bounds: Array[Double]): Seq[Int] = {
    val res = new mutable.HashSet[Int]()
    if (interval != null){
      val start = range_bounds.indexWhere(x => x >= interval.min._1)
      var end = range_bounds.indexWhere(x => x >= interval.max._1)
      if (end == -1) end = range_bounds.length
      if (start >= 0) {
        for (i <- start to end + 1)
          res.add(i)
      } else res.add(range_bounds.length)
    }
    res.toArray
  }
}
