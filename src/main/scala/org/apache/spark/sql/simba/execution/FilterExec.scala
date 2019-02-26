/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.simba.execution

import org.apache.spark.sql.simba.expression._
import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.util.ShapeUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.{SortOrder, And => SQLAnd, Not => SQLNot, Or => SQLOr}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by dongx on 11/13/2016.
  */
case class FilterExec(condition: Expression, child: SparkPlan) extends SimbaPlan with PredicateHelper {
   override def output: Seq[Attribute] = child.output

  private class DistanceOrdering(point: Expression, target: Point) extends Ordering[InternalRow] {
    override def compare(x: InternalRow, y: InternalRow): Int = {
      val shape_x = ShapeUtils.getShape(point, child.output, x)
      val shape_y = ShapeUtils.getShape(point, child.output, y)
      val dis_x = target.minDist(shape_x)
      val dis_y = target.minDist(shape_y)
      dis_x.compare(dis_y)
    }
  }

  // TODO change target partition from 1 to some good value
  // Note that target here must be an point literal in WHERE clause,
  // hence we can consider it as Point safely
  def knn(rdd: RDD[InternalRow], point: Expression, target: Point, k: Int): RDD[InternalRow] =
    sparkContext.parallelize(rdd.map(_.copy()).takeOrdered(k)(new DistanceOrdering(point, target)), 1)

  def applyCondition(rdd: RDD[InternalRow], condition: Expression): RDD[InternalRow] = {
    condition match {
      case InKNN(point, target, k) =>
        val _target = target.asInstanceOf[Literal].value.asInstanceOf[Point]
        knn(rdd, point, _target, k.value.asInstanceOf[Number].intValue())
      case now@And(left, right) =>
        if (!now.hasKNN) rdd.mapPartitions{ iter => iter.filter(newPredicate(condition, child.output).eval(_))}
        else applyCondition(rdd, left).map(_.copy()).intersection(applyCondition(rdd, right).map(_.copy()))
      case now@Or(left, right) =>
        if (!now.hasKNN) rdd.mapPartitions{ iter => iter.filter(newPredicate(condition, child.output).eval(_))}
        else applyCondition(rdd, left).map(_.copy()).union(applyCondition(rdd, right).map(_.copy())).distinct()
      case now@Not(c) =>
        if (!now.hasKNN) rdd.mapPartitions{ iter => iter.filter(newPredicate(condition, child.output).eval(_))}
        else rdd.map(_.copy()).subtract(applyCondition(rdd, c).map(_.copy()))
      case _ =>
        rdd.mapPartitions(iter => iter.filter(newPredicate(condition, child.output).eval(_)))
    }
  }

  protected def doExecute(): RDD[InternalRow] = {
    val root_rdd = child.execute()
    condition transformUp {
      case SQLAnd(left, right) => And(left, right)
      case SQLOr(left, right)=> Or(left, right)
      case SQLNot(c) => Not(c)
    }
    applyCondition(root_rdd, condition)
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def children: Seq[SparkPlan] = child :: Nil
  override def outputPartitioning: Partitioning = child.outputPartitioning
}
