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

package edu.utah.cs.simba

import edu.utah.cs.simba.execution.QueryExecution
import edu.utah.cs.simba.expression.{InCircleRange, InKNN, InRange, PointWrapper}
import edu.utah.cs.simba.plans.{DistanceJoin, KNNJoin}
import edu.utah.cs.simba.spatial.Point
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}

/**
  * Created by gefei on 2016/11/14.
  */
private[simba] class DataFrame (@transient val simbaContext: SimbaContext,
                              @transient override val queryExecution: QueryExecution
                              ) extends sql.DataFrame(simbaContext, queryExecution.withIndexedData) {

  def this(simbaContext: SimbaContext, logicalPlan: LogicalPlan) = this(simbaContext, simbaContext.executePlan(logicalPlan))

  /**
    * Spatial operation, range query.
    * {{{
    *   point.range(Array("x", "y"), Array(10, 10), Array(20, 20))
    *   point.filter($"x" >= 10 && $"x" <= 20 && $"y" >= 10 && $"y" <= 20)
    * }}}
    */
  def range(keys: Array[String], point1: Array[Double], point2: Array[Double]): DataFrame =
  withPlan {
    val attrs = getAttributes(keys)
    attrs.foreach(attr => assert(attr != null, "column not found"))

    Filter(InRange(PointWrapper(attrs),
      Literal(new Point(point1)),
      Literal(new Point(point2))), logicalPlan)
  }

  /**
    * Spatial operation, range query
    * {{{
    *   point.range(p, Array(10, 10), Array(20, 20))
    * }}}
    */
  def range(key: String, point1: Array[Double], point2: Array[Double]): DataFrame =
  withPlan {
    val attrs = getAttributes(Array(key))
    assert(attrs.head != null, "column not found")

    Filter(InRange(attrs.head,
      Literal(new Point(point1)),
      Literal(new Point(point2))), logicalPlan)
  }

  /**
    * Spatial operation knn
    * Find k nearest neighbor of a given point
    */
  def knn(keys: Array[String], point: Array[Double], k: Int): DataFrame = withPlan{
    val attrs = getAttributes(keys)
    attrs.foreach(attr => assert(attr != null, "column not found"))
    Filter(InKNN(PointWrapper(attrs),
      Literal(new Point(point)), Literal(k)), logicalPlan)
  }

  def knn(key: String, point: Array[Double], k: Int): DataFrame = withPlan{
    val attrs = getAttributes(Array(key))
    assert(attrs.head != null, "column not found")
    Filter(InKNN(attrs.head,
      Literal(new Point(point)), Literal(k)), logicalPlan)
  }

  /**
    * Spatial operation circle range query
    * {{{
    *   point.circleRange(Array("x", "y"), Array(10, 10), 5)
    *   point.filter(($"x" - 10) * ($"x" - 10) + ($"y" - 10) * ($"y" - 10) <= 5 * 5)
    * }}}
    */
  def circleRange(keys: Array[String], point: Array[Double], r: Double): DataFrame = withPlan {
    val attrs = getAttributes(keys)
    attrs.foreach(attr => assert(attr != null, "column not found"))
    Filter(InCircleRange(PointWrapper(attrs),
      Literal(new Point(point)),
      Literal(r)), logicalPlan)
  }

//  /**
//    * Spatial operation DistanceJoin
//    */
//  def distanceJoin(right: DataFrame, leftKeys: Array[String],
//                   rightKeys: Array[String], r: Double) : DataFrame = withPlan {
//    val leftAttrs = getAttributes(leftKeys)
//    val rightAttrs = getAttributes(rightKeys, right.queryExecution.analyzed.output)
//    Join(this.logicalPlan, right.logicalPlan, DistanceJoin,
//      Some(InCircleRange(PointWrapper(rightAttrs),
//        PointWrapper(leftAttrs),
//        Literal(r))))
//  }
//
//  /**
//    * Spatial operation KNNJoin
//    */
//  def knnJoin(right: DataFrame, leftKeys: Array[String],
//              rightKeys: Array[String], k : Int) : DataFrame = withPlan {
//    val leftAttrs = getAttributes(leftKeys)
//    val rightAttrs = getAttributes(rightKeys, right.queryExecution.analyzed.output)
//    Join(this.logicalPlan, right.logicalPlan, KNNJoin,
//      Some(InKNN(PointWrapper(rightAttrs),
//        PointWrapper(leftAttrs), Literal(k))))
//  }
//
//  def knnJoin(right: DataFrame, leftKey: String,
//              rightKey: String, k : Int) : DataFrame = withPlan {
//    val leftAttrs = getAttributes(Array(leftKey))
//    val rightAttrs = getAttributes(Array(rightKey), right.queryExecution.analyzed.output)
//    Join(this.logicalPlan, right.logicalPlan, KNNJoin,
//      Some(InKNN(rightAttrs.head,
//        leftAttrs.head, Literal(k))))
//  }

  /**
    * a private auxiliary function to extract Attribute information from an input string
    * object.
    *
    * @param keys  An array of String input by user
    * @param attrs A Seq of Attributes in which to search
    * @return An Array of Attribute extracted from the String
    */
  private def getAttributes(keys: Array[String],
                            attrs: Seq[Attribute] = this.queryExecution.analyzed.output)
  : Array[Attribute] = {
    keys.map(key => {
      val temp = attrs.indexWhere(_.name == key)
      if (temp >= 0) attrs(temp)
      else null
    })
  }

  private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    new DataFrame(simbaContext, logicalPlan)
  }
}