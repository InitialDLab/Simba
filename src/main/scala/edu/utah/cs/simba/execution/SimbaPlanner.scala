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

import edu.utah.cs.simba.{IndexInfo, SimbaContext}
import edu.utah.cs.simba.execution.join._
import edu.utah.cs.simba.expression.{InCircleRange, InKNN, InRange, PointWrapper}
import edu.utah.cs.simba.index.{IndexedRelation, IndexedRelationScan}
import edu.utah.cs.simba.plans._
import edu.utah.cs.simba.util.PredicateUtil
import org.apache.spark.Logging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner}

/**
  * Created by dongx on 11/13/2016.
  */
class SimbaPlanner(val simbaContext: SimbaContext) extends SparkPlanner(simbaContext) {
  override def strategies: Seq[Strategy] =
    super.strategies ++ (
      SpatialJoinExtractor ::
        IndexRelationScans ::
        SimbaFilter :: Nil)

  object IndexRelationScans extends Strategy with PredicateHelper{
    import org.apache.spark.sql.catalyst.expressions._
    lazy val indexInfos = simbaContext.indexManager.getIndexInfo

    def lookupIndexInfo(attributes: Seq[Attribute]): IndexInfo =
      indexInfos.find(item => attributes.count(item.attributes.contains)
        == attributes.length).orNull

    def mapIndexedExpression(expression: Expression): Expression = {
      val tmp_exp = expression match {
        case LessThan(left: NamedExpression, right: Literal) =>
          val attrs = Array(left.toAttribute)
          if (lookupIndexInfo(attrs) != null) LessThanOrEqual(left, right)
          else null
        case GreaterThan(left: NamedExpression, right: Literal) =>
          val attrs = Array(left.toAttribute)
          if (lookupIndexInfo(attrs) != null) GreaterThanOrEqual(left, right)
          else null
        case _ =>
          null
      }

      if (tmp_exp == null) {
        val attrs: Seq[Attribute] = expression match {
          case EqualTo(left: NamedExpression, right: Literal) =>
            Array(left.toAttribute)
          case LessThanOrEqual(left: NamedExpression, right: Literal) =>
            Array(left.toAttribute)
          case GreaterThanOrEqual(left: NamedExpression, right: Literal) =>
            Array(left.toAttribute)

          case InRange(point: Expression, _, _) =>
            point match {
              case wrapper: PointWrapper =>
                wrapper.exps.map(_.asInstanceOf[NamedExpression].toAttribute)
              case p =>
                Array(p.asInstanceOf[NamedExpression].toAttribute)
            }
          case InKNN(point: Expression, target: Expression, k: Literal) =>
            point match {
              case wrapper: PointWrapper =>
                wrapper.exps.map(_.asInstanceOf[NamedExpression].toAttribute)
              case p =>
                Array(p.asInstanceOf[NamedExpression].toAttribute)
            }
          case InCircleRange(point: Expression, target: Expression, r: Literal) =>
            point match {
              case wrapper: PointWrapper =>
                wrapper.exps.map(_.asInstanceOf[NamedExpression].toAttribute)
              case p =>
                Array(p.asInstanceOf[NamedExpression].toAttribute)
            }
          case _ =>
            null
        }
        if (attrs != null && lookupIndexInfo(attrs) != null) expression
        else null
      } else tmp_exp
    }

    def selectFilter(predicates: Seq[Expression]): Seq[Expression] = {
      val originalPredicate = predicates.reduceLeftOption(And).getOrElse(Literal(true))
      val clauses = PredicateUtil.splitDNFPredicates(originalPredicate).map(PredicateUtil.splitCNFPredicates)
      val predicateCanBeIndexed = clauses.map(clause =>
        clause.map(mapIndexedExpression).filter(_ != null))
      predicateCanBeIndexed.map(pre => pre.reduceLeftOption(And).getOrElse(Literal(true)))
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, indexed: IndexedRelation) =>
        val predicatesCanBeIndexed = selectFilter(filters)
        val parentFilter = // if all predicate can be indexed, then remove the predicate
          if (predicatesCanBeIndexed.toString // TODO ugly hack
            .compareTo(Seq(filters.reduceLeftOption(And).getOrElse(true)).toString) == 0) Seq[Expression]()
          else filters
        pruneFilterProject(
          projectList,
          parentFilter,
          identity[Seq[Expression]],
          IndexedRelationScan(_, predicatesCanBeIndexed, indexed)) :: Nil
      case _ => Nil
    }
  }

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

  object SimbaFilter extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Filter(condition, child) =>
        Filter(condition, planLater(child)) :: Nil
      case _ => Nil
    }
  }
}
