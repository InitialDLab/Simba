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

package org.apache.spark.sql.simba.plans

import org.apache.spark.sql.simba.expression.{InCircleRange, InKNN}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan}
import org.apache.spark.sql.types.BooleanType

/**
  * Created by dongx on 11/13/2016.
  */
case class SpatialJoin(left: LogicalPlan, right: LogicalPlan, joinType: SpatialJoinType,
                       condition: Option[Expression]) extends BinaryNode {
  override def output: Seq[Attribute] = {
    joinType match {
      case KNNJoin =>
        require(condition.get.isInstanceOf[InKNN])
        left.output ++ right.output
      case ZKNNJoin =>
        require(condition.get.isInstanceOf[InKNN])
        left.output ++ right.output
      case DistanceJoin =>
        require(condition.get.isInstanceOf[InCircleRange])
        left.output ++ right.output.map(_.withNullability(true))
      case _ =>
        left.output ++ right.output
    }
  }

  def selfJoinResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  // Joins are only resolved if they don't introduce ambiguous expression ids.
  override lazy val resolved: Boolean = {
    childrenResolved &&
      expressions.forall(_.resolved) &&
      selfJoinResolved &&
      condition.forall(_.dataType == BooleanType)
  }
}
