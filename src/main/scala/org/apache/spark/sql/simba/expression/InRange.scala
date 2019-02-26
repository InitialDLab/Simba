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
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.simba.spatial.{MBR, Point, Shape}
import org.apache.spark.sql.simba.util.ShapeUtils
import org.apache.spark.sql.catalyst.util.GenericArrayData

/**
  * Created by dongx on 11/10/16.
  */
case class InRange(shape: Expression, range_low: Expression, range_high: Expression)
  extends Predicate with CodegenFallback{
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val eval_shape = ShapeUtils.getShape(shape, input)
    val eval_low = range_low.asInstanceOf[Literal].value.asInstanceOf[Point]
    val eval_high = range_high.asInstanceOf[Literal].value.asInstanceOf[Point]
    require(eval_shape.dimensions == eval_low.dimensions && eval_shape.dimensions == eval_high.dimensions)
    val mbr = MBR(eval_low, eval_high)
    mbr.intersects(eval_shape)
  }

  override def toString: String = s" **($shape) IN Rectangle ($range_low) - ($range_high)**  "

  override def children: Seq[Expression] = Seq(shape, range_low, range_high)
}
