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

package edu.utah.cs.simba.execution

import edu.utah.cs.simba.SimbaContext
import edu.utah.cs.simba.execution.join._
import edu.utah.cs.simba.plans._
import org.apache.spark.Logging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner}

/**
  * Created by dongx on 11/13/2016.
  */
class SimbaPlanner(val simbaContext: SimbaContext) extends SparkPlanner(simbaContext) {
  //XX very bad hack to get through private scope (for DataSourceStrategy)
  override def strategies: Seq[Strategy] =
    super.strategies.take(simbaContext.experimental.extraStrategies.size + 1) ++ (
        DDLStrategy ::
        TakeOrderedAndProject ::
        SpatialJoinExtractor ::
        Aggregation ::
        LeftSemiJoin ::
        EquiJoinSelection ::
        InMemoryScans ::
        BasicOperators ::
        BroadcastNestedLoop ::
        CartesianProduct ::
        DefaultJoin :: Nil)

  object ExtractSpatialJoinKeys extends Logging with PredicateHelper {
    type ReturnType = (Expression, Expression, Literal, SpatialJoinType, LogicalPlan, LogicalPlan)

    def unapply(plan: LogicalPlan): Option[ReturnType] = {
      plan match {
        case SpatialJoin(left, right, KNNJoin, condition) =>
          val children = condition.get.children
          require(children.size == 3)
          val right_key = children.head
          val left_key = children(1)
          val k = children.last.asInstanceOf[Literal]
          Some((left_key, right_key, k, KNNJoin, left, right))
        case SpatialJoin(left, right, ZKNNJoin, condition) =>
          val children = condition.get.children
          require(children.size == 3)
          val right_key = children.head
          val left_key = children(1)
          val k = children.last.asInstanceOf[Literal]
          Some((left_key, right_key, k, ZKNNJoin, left, right))
        case SpatialJoin(left, right, DistanceJoin, condition) =>
          val children = condition.get.children
          require(children.size == 3)
          val right_key = children.head
          val left_key = children(1)
          val k = children.last.asInstanceOf[Literal]
          Some((left_key, right_key, k, DistanceJoin, left, right))
        case _ => None
      }
    }
  }

  object SpatialJoinExtractor extends Strategy with PredicateHelper{
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ExtractSpatialJoinKeys(leftKey, rightKey, k, KNNJoin, left, right) =>
        simbaContext.simbaConf.knnJoin match {
          case "RKJSpark" =>
            RKJSpark(leftKey, rightKey, k, planLater(left), planLater(right)) :: Nil
          case "CKJSpark" =>
            CKJSpark(leftKey, rightKey, k, planLater(left), planLater(right)) :: Nil
          case "VKJSpark" =>
            VKJSpark(leftKey, rightKey, k, planLater(left), planLater(right)) :: Nil
          case "BKJSpark" =>
            BKJSpark(leftKey, rightKey, k, planLater(left), planLater(right)) :: Nil
          case "BKJSpark-R" =>
            BKJSparkR(leftKey, rightKey, k, planLater(left), planLater(right)) :: Nil
          case _ =>
            RKJSpark(leftKey, rightKey, k, planLater(left), planLater(right)) :: Nil
        }
      case ExtractSpatialJoinKeys(leftKey, rightKey, k, ZKNNJoin, left, right) =>
        ZKJSpark(leftKey, rightKey, k, planLater(left), planLater(right)) :: Nil
      case ExtractSpatialJoinKeys(leftKey, rightKey, r, DistanceJoin, left, right) =>
        simbaContext.simbaConf.distanceJoin match {
          case "RDJSpark" =>
            RDJSpark(leftKey, rightKey, r, planLater(left), planLater(right)) :: Nil
          case "DJSpark" =>
            DJSpark(leftKey, rightKey, r, planLater(left), planLater(right)) :: Nil
          case "CDJSpark" =>
            CDJSpark(leftKey, rightKey, r, planLater(left), planLater(right)) :: Nil
          case "BDJSpark" =>
            BDJSpark(leftKey, rightKey, r, planLater(left), planLater(right)) :: Nil
          case "BDJSpark-R" =>
            BDJSparkR(leftKey, rightKey, r, planLater(left), planLater(right)) :: Nil
          case _ =>
            RDJSpark(leftKey, rightKey, r, planLater(left), planLater(right)) :: Nil
        }
      case _ => Nil
    }
  }
}
