/*
 * Copyright 2017 by Simba Project
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
 *
 */

package org.apache.spark.sql.simba.expression

import org.apache.spark.sql.simba.{ShapeSerializer, ShapeType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.simba.spatial.{Circle, Point, Shape}
import org.apache.spark.sql.simba.util.{NumberUtil, ShapeUtils}
import org.apache.spark.sql.catalyst.util.GenericArrayData

/**
  * Created by dongx on 11/10/16.
  */
case class InCircleRange(shape: Expression, target: Expression, radius: Literal)
  extends Predicate with CodegenFallback {
  require(radius.dataType.isInstanceOf[NumericType])

  override def children: Seq[Expression] = Seq(shape, target, radius)

  override def nullable: Boolean = false

  override def toString: String = s" **($shape) IN CIRCLERANGE ($target) within  ($radius)**  "

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = {
    val eval_shape = ShapeUtils.getShape(shape, input)
    val eval_target = target.eval(input).asInstanceOf[Point]
    require(eval_shape.dimensions == eval_target.dimensions)
    val eval_r = NumberUtil.literalToDouble(radius)
    Circle(eval_target, eval_r).intersects(eval_shape)
  }
}
