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

package org.apache.spark.sql.simba

import org.apache.spark.sql.ExperimentalMethods
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.simba.plans.SpatialJoin

/**
  * Created by dongx on 3/7/17.
  */
class SimbaOptimizer(catalog: SessionCatalog,
                     conf: SQLConf,
                     experimentalMethods: ExperimentalMethods)
 extends SparkOptimizer(catalog, conf, experimentalMethods) {
  override def batches: Seq[Batch] = super.batches :+
    Batch("SpatialJoinPushDown", FixedPoint(100), PushPredicateThroughSpatialJoin)
}

object PushPredicateThroughSpatialJoin extends Rule[LogicalPlan] with PredicateHelper {
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftEvaluateCondition, rest) =
      condition.partition(_.references subsetOf left.outputSet)
    val (rightEvaluateCondition, commonCondition) =
      rest.partition(_.references subsetOf right.outputSet)

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, SpatialJoin(left, right, joinType, joinCondition)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)

      val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
      val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
      val newJoinCond = (commonFilterCondition ++ joinCondition).reduceLeftOption(And)
      SpatialJoin(newLeft, newRight, joinType, newJoinCond)

    // push down the join filter into sub query scanning if applicable
    case f @ SpatialJoin(left, right, joinType, joinCondition) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
      val newRight = rightJoinConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
      val newJoinCond = commonJoinCondition.reduceLeftOption(And)

      SpatialJoin(newLeft, newRight, joinType, newJoinCond)
  }
}
