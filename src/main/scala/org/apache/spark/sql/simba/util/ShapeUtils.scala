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

package org.apache.spark.sql.simba.util

import org.apache.spark.sql.simba.{ShapeSerializer, ShapeType}
import org.apache.spark.sql.simba.expression.PointWrapper
import org.apache.spark.sql.simba.spatial.{Point, Shape}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by dongx on 11/12/2016.
  */
object ShapeUtils {
  def getPointFromRow(row: InternalRow, columns: List[Attribute], plan: SparkPlan,
                      isPoint: Boolean): Point = {
    if (isPoint) {
      ShapeSerializer.deserialize(BindReferences.bindReference(columns.head, plan.output)
        .eval(row).asInstanceOf[UnsafeArrayData].toByteArray).asInstanceOf[Point]
    } else {
      Point(columns.toArray.map(BindReferences.bindReference(_, plan.output).eval(row)
        .asInstanceOf[Number].doubleValue()))
    }
  }
  def getPointFromRow(row: InternalRow, columns: List[Attribute], plan: LogicalPlan,
                      isPoint: Boolean): Point = {
    if (isPoint) {
      ShapeSerializer.deserialize(BindReferences.bindReference(columns.head, plan.output)
        .eval(row).asInstanceOf[UnsafeArrayData].toByteArray).asInstanceOf[Point]
    } else {
      Point(columns.toArray.map(BindReferences.bindReference(_, plan.output).eval(row)
        .asInstanceOf[Number].doubleValue()))
    }
  }

  def getShape(expression: Expression, input: InternalRow): Shape = {
    if (!expression.isInstanceOf[PointWrapper] && expression.dataType.isInstanceOf[ShapeType]) {
      ShapeSerializer.deserialize(expression.eval(input).asInstanceOf[UnsafeArrayData].toByteArray)
    } else if (expression.isInstanceOf[PointWrapper]) {
      expression.eval(input).asInstanceOf[Shape]
    } else throw new UnsupportedOperationException("Query shape should be of ShapeType")
  }

  def getShape(expression: Expression, schema: Seq[Attribute], input: InternalRow): Shape = {
    if (!expression.isInstanceOf[PointWrapper] && expression.dataType.isInstanceOf[ShapeType]) {
      ShapeSerializer.deserialize(BindReferences.bindReference(expression, schema)
        .eval(input).asInstanceOf[UnsafeArrayData].toByteArray)
    } else if (expression.isInstanceOf[PointWrapper]) {
      BindReferences.bindReference(expression, schema).eval(input).asInstanceOf[Shape]
    } else throw new UnsupportedOperationException("Query shape should be of ShapeType")
  }

}
