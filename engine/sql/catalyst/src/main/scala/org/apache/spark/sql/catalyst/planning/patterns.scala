/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.sql.types.{Metadata, DoubleType}

import scala.annotation.tailrec

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._

import scala.xml.MetaData

/**
 * A pattern that matches any number of filter operations on top of another relational operator.
 * Adjacent filter operators are collected and their conditions are broken up and returned as a
 * sequence of conjunctive predicates.
 *
 * @return A tuple containing a sequence of conjunctive predicates that should be used to filter the
 *         output and a relational operator.
 */
object FilteredOperation extends PredicateHelper {
  type ReturnType = (Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = Some(collectFilters(Nil, plan))

  @tailrec
  private def collectFilters(filters: Seq[Expression], plan: LogicalPlan): ReturnType = plan match {
    case Filter(condition, child) =>
      collectFilters(filters ++ splitConjunctivePredicates(condition), child)
    case other => (filters, other)
  }
}

//object IndexedOperation extends PredicateHelper{
//  type ReturnType = (Seq[Expression], LogicalPlan)
//
//  def collectFilters(plan: LogicalPlan):
//  (Seq[Expression], LogicalPlan, Map[Attribute, Expression]) =
//    plan match {
//      case Filter(condition, child) =>
//        val (filters, other, aliases) = collectFilters(child)
//        val substitutedCondition = substitute(aliases)(condition)
//        (filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)
//
//      case other =>
//        (Nil, other, Map.empty)
//    }
//
//  def substitute(aliases: Map[Attribute, Expression])(expr: Expression) = expr.transform {
//    case a @ Alias(ref: AttributeReference, name) =>
//      aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)
//
//    case a: AttributeReference =>
//      aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
//  }
//
//  def unapply(plan: LogicalPlan): Option[ReturnType] = {
//    val (filters, child, _) = collectFilters(plan)
//    // TODO Select the filters can be indexed here
//    Some(filters, child)
//  }
//}

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperation extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _) = collectProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child))
  }


  //[[FOR DEBUGING]]
//  // divide dnf to an array of conjunctive clauses
//  def dnfExtract(expression: Expression) : Array[Expression] = {
//    expression match {
//      case Or(left, right) =>
//        dnfExtract(left) ++ dnfExtract(right)
//      case And(left @ And(l2, r2), right) =>
//        dnfExtract(And(l2, And(l2, right)))
//      //        Array(conjExtract(left) ++ conjExtract(right))
//      case other =>
//        Array(other)
//    }
//  }
//  def dnfExtract(expression: Expression) : Array[Array[Expression]] = {
//    expression match {
//      case Or(left, right) =>
//        dnfExtract(left) ++ dnfExtract(right)
//      case And(left, right) =>
//        Array(conjExtract(left) ++ conjExtract(right))
//      case other =>
//        Array(Array(other))
//    }
//  }
//
//  def conjExtract(expression: Expression) : Array[Expression] = {
//    expression match {
//      case And(left, right) =>
//        conjExtract(left) ++ conjExtract(right)
//      case other @ _ =>
//        Array(other)
//    }
//  }

//  trait Index
//  object IndexType {
//    def apply(typ : String) : IndexType = typ.toLowerCase match {
//      case "rtree" => RTreeType
//      case "treemap" => TreeMapType
//      case "hashmap" => HashMapType
//      case _ => null
//    }
//  }
//
//  sealed abstract class IndexType
//
//  case object RTreeType extends IndexType
//
//  case object TreeMapType extends IndexType
//
//  case object HashMapType extends IndexType
//
//  case class IndexInfo(attributes: Array[Attribute], index: IndexType)
//
//  //  var indexOperations: Array[Array[Expression]] = null  // a Sequence stores the predicates can be processed by indexes
//  val attr_x = AttributeReference("x", DoubleType)(ExprId(1l))
//  val attr_y = AttributeReference("y", DoubleType)(ExprId(2l))
//  var indexInfos = Array[IndexInfo](IndexInfo(Array(attr_x, attr_y), RTreeType))  // a sequence gather the index information from IndexManager

  /**
   * Collects projects and filters, in-lining/substituting aliases if necessary.  Here are two
   * examples for alias in-lining/substitution.  Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  def collectProjectsAndFilters(plan: LogicalPlan):
      (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression]) =
    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))

      case Filter(condition, child) =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

      case other =>
        (None, Nil, other, Map.empty)
    }

  def collectAliases(fields: Seq[Expression]) = fields.collect {
    case a @ Alias(child, _) => a.toAttribute.asInstanceOf[Attribute] -> child
  }.toMap

  def substitute(aliases: Map[Attribute, Expression])(expr: Expression) = expr.transform {
    case a @ Alias(ref: AttributeReference, name) =>
      aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

    case a: AttributeReference =>
      aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
  }
}

/**
 * Matches a logical aggregation that can be performed on distributed data in two steps.  The first
 * operates on the data in each partition performing partial aggregation for each group.  The second
 * occurs after the shuffle and completes the aggregation.
 *
 * This pattern will only match if all aggregate expressions can be computed partially and will
 * return the rewritten aggregation expressions for both phases.
 *
 * The returned values for this match are as follows:
 *  - Grouping attributes for the final aggregation.
 *  - Aggregates for the final aggregation.
 *  - Grouping expressions for the partial aggregation.
 *  - Partial aggregate expressions.
 *  - Input to the aggregation.
 */
object PartialAggregation {
  type ReturnType =
    (Seq[Attribute], Seq[NamedExpression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case logical.Aggregate(groupingExpressions, aggregateExpressions, child) =>
      // Collect all aggregate expressions.
      val allAggregates =
        aggregateExpressions.flatMap(_ collect { case a: AggregateExpression => a})
      // Collect all aggregate expressions that can be computed partially.
      val partialAggregates =
        aggregateExpressions.flatMap(_ collect { case p: PartialAggregate => p})

      // Only do partial aggregation if supported by all aggregate expressions.
      if (allAggregates.size == partialAggregates.size) {
        // Create a map of expressions to their partial evaluations for all aggregate expressions.
        val partialEvaluations: Map[TreeNodeRef, SplitEvaluation] =
          partialAggregates.map(a => (new TreeNodeRef(a), a.asPartial)).toMap

        // We need to pass all grouping expressions though so the grouping can happen a second
        // time. However some of them might be unnamed so we alias them allowing them to be
        // referenced in the second aggregation.
        val namedGroupingExpressions: Map[Expression, NamedExpression] =
          groupingExpressions.filter(!_.isInstanceOf[Literal]).map {
            case n: NamedExpression => (n, n)
            case other => (other, Alias(other, "PartialGroup")())
          }.toMap

        // Replace aggregations with a new expression that computes the result from the already
        // computed partial evaluations and grouping values.
        val rewrittenAggregateExpressions = aggregateExpressions.map(_.transformUp {
          case e: Expression if partialEvaluations.contains(new TreeNodeRef(e)) =>
            partialEvaluations(new TreeNodeRef(e)).finalEvaluation

          case e: Expression =>
            // Should trim aliases around `GetField`s. These aliases are introduced while
            // resolving struct field accesses, because `GetField` is not a `NamedExpression`.
            // (Should we just turn `GetField` into a `NamedExpression`?)
            namedGroupingExpressions
              .get(e.transform { case Alias(g: GetField, _) => g })
              .map(_.toAttribute)
              .getOrElse(e)
        }).asInstanceOf[Seq[NamedExpression]]

        val partialComputation =
          (namedGroupingExpressions.values ++
            partialEvaluations.values.flatMap(_.partialEvaluations)).toSeq

        val namedGroupingAttributes = namedGroupingExpressions.values.map(_.toAttribute).toSeq

        Some(
          (namedGroupingAttributes,
           rewrittenAggregateExpressions,
           groupingExpressions,
           partialComputation,
           child))
      } else {
        None
      }
    case _ => None
  }
}


/**
 * A pattern that finds joins with equality conditions that can be evaluated using equi-join.
 */
object ExtractEquiJoinKeys extends Logging with PredicateHelper {
  /** (joinType, rightKeys, leftKeys, condition, leftChild, rightChild) */
  type ReturnType =
    (JoinType, Seq[Expression], Seq[Expression], Option[Expression], LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case join @ Join(left, right, joinType, condition) =>
      logDebug(s"Considering join on: $condition")
      // Find equi-join predicates that can be evaluated before the join, and thus can be used
      // as join keys.
      val (joinPredicates, otherPredicates) =
        condition.map(splitConjunctivePredicates).getOrElse(Nil).partition {
          case EqualTo(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
            (canEvaluate(l, right) && canEvaluate(r, left)) => true
          case _ => false
        }

      val joinKeys = joinPredicates.map {
        case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => (l, r)
        case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => (r, l)
      }
      val leftKeys = joinKeys.map(_._1)
      val rightKeys = joinKeys.map(_._2)

      if (joinKeys.nonEmpty) {
        logDebug(s"leftKeys:$leftKeys | rightKeys:$rightKeys")
        Some((joinType, leftKeys, rightKeys, otherPredicates.reduceOption(And), left, right))
      } else {
        None
      }
    case _ => None
  }
}

object ExtractSpatialJoinKeys extends Logging with PredicateHelper{
  type ReturnType = (Seq[Expression], Seq[Expression], Literal, JoinType, LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    if (plan.isInstanceOf[Join] && plan.asInstanceOf[Join].isSpatialJoin) {
      plan match{
        case Join(left, right, KNNJoin, condition) =>
          val children = condition.get.children
          val len = (children.length - 1) >> 1
          var leftKeys = Seq[Expression]()
          var rightKeys = Seq[Expression]()
          for (i <-0 to len - 1) {
            rightKeys = rightKeys :+ children(i)
            leftKeys = leftKeys :+ children(i + len)
          }
          val k = children(children.length - 1).asInstanceOf[Literal]
          Some((leftKeys, rightKeys, k, KNNJoin, left, right))
        case Join(left, right, zKNNJoin, condition) =>
          val children = condition.get.children
          val len = (children.length - 1) >> 1
          var leftKeys = Seq[Expression]()
          var rightKeys = Seq[Expression]()
          for (i <-0 to len - 1) {
            rightKeys = rightKeys :+ children(i)
            leftKeys = leftKeys :+ children(i + len)
          }
          val k = children(children.length - 1).asInstanceOf[Literal]
          Some((leftKeys, rightKeys, k, zKNNJoin, left, right))
        case Join(left, right, DistanceJoin, condition) =>
          val children = condition.get.children
          val len = (children.length - 1) >> 1
          var leftKeys = Seq[Expression]()
          var rightKeys = Seq[Expression]()
          for (i <-0 to len - 1) {
            rightKeys = rightKeys :+ children(i)
            leftKeys = leftKeys :+ children(i + len)
          }
          val r = children(children.length - 1).asInstanceOf[Literal]
          Some((leftKeys, rightKeys, r, DistanceJoin, left, right))
      }
    } else None

  }
}

/**
 * A pattern that collects all adjacent unions and returns their children as a Seq.
 */
object Unions {
  def unapply(plan: LogicalPlan): Option[Seq[LogicalPlan]] = plan match {
    case u: Union => Some(collectUnionChildren(u))
    case _ => None
  }

  private def collectUnionChildren(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case Union(l, r) => collectUnionChildren(l) ++ collectUnionChildren(r)
    case other => other :: Nil
  }
}
