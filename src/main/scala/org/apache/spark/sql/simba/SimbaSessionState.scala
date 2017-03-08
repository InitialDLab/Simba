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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Strategy, catalyst}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Literal, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, SparkPlanner}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.simba.execution.FilterExec
import org.apache.spark.sql.simba.execution.join.{BDJSparkR, _}
import org.apache.spark.sql.simba.expression.{InCircleRange, InKNN, InRange, PointWrapper}
import org.apache.spark.sql.simba.index.{IndexedRelation, IndexedRelationScan}
import org.apache.spark.sql.simba.plans._
import org.apache.spark.sql.simba.util.PredicateUtil

import scala.collection.immutable

/**
  * Created by dongx on 3/7/17.
  */
private[simba] class SimbaSessionState(simbaSession: SimbaSession) extends SessionState(simbaSession) { self =>
  protected[simba] lazy val simbaConf = new SimbaConf


  protected[simba] val indexManager: IndexManager = new IndexManager

  override lazy val optimizer = new SimbaOptimizer(catalog, conf, experimentalMethods)

  override def executePlan(plan: LogicalPlan) =
    new execution.QueryExecution(simbaSession, plan)

  def setConf(key: String, value: String): Unit = {
    if (key.startsWith("simba.")) simbaConf.setConfString(key, value)
    else conf.setConfString(key, value)
  }

  def getConf(key: String): String = {
    if (key.startsWith("simba.")) simbaConf.getConfString(key)
    else conf.getConfString(key)
  }

  def getConf(key: String, defaultValue: String): String = {
    if (key.startsWith("simba.")) conf.getConfString(key, defaultValue)
    else conf.getConfString(key, defaultValue)
  }

  def getAllConfs: immutable.Map[String, String] = {
    conf.getAllConfs ++ simbaConf.getAllConfs
  }

  override def planner: SparkPlanner = {
    new SparkPlanner(simbaSession.sparkContext, conf,
      (SpatialJoinExtractor :: IndexRelationScans :: SimbaFilter :: Nil) ++ experimentalMethods.extraStrategies)
  }

  object IndexRelationScans extends Strategy with PredicateHelper {
    import org.apache.spark.sql.catalyst.expressions._
    lazy val indexInfos = indexManager.getIndexInfo

    def lookupIndexInfo(attributes: Seq[Attribute]): IndexInfo =
      indexInfos
        .find(item => attributes.count(now => item.attributes.exists(x => x.semanticEquals(now))) == attributes.length)
        .orNull

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
        pruneFilterProjectionForIndex(
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

  object SpatialJoinExtractor extends Strategy with PredicateHelper {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ExtractSpatialJoinKeys(leftKey, rightKey, k, KNNJoin, left, right) =>
        simbaConf.knnJoin match {
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
        simbaConf.distanceJoin match {
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
        FilterExec(condition, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  def pruneFilterProjectionForIndex(projectList: Seq[NamedExpression], filterPredicates: Seq[Expression],
                                    prunePushedDownFilters: Seq[Expression] => Seq[Expression],
                                    scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {
    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition: Option[Expression] =
      prunePushedDownFilters(filterPredicates).reduceLeftOption(catalyst.expressions.And)

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
      filterSet.subsetOf(projectSet)) {
      val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
      filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      val scan = scanBuilder((projectSet ++ filterSet).toSeq)
      ProjectExec(projectList, filterCondition.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }

}
