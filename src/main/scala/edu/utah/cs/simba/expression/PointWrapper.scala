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
 *
 */

package edu.utah.cs.simba.expression

import edu.utah.cs.simba.ShapeType
import edu.utah.cs.simba.spatial.Point

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

/**
  * Created by dongx on 11/10/16.
  */
case class PointWrapper(exps: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def dataType: DataType = ShapeType

  override def children: Seq[Expression] = exps

  override def eval(input: InternalRow): Any = {
    val coord = exps.map(_.eval(input).asInstanceOf[Double]).toArray
    Point(coord)
  }
}
