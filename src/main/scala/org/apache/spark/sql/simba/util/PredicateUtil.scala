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

import org.apache.spark.sql.catalyst.expressions.{Expression, And, Or}

/**
  * Created by gefei on 2016/11/15.
  */
object PredicateUtil {
  def toDNF(condition: Expression): Expression = {
    condition match {
      case Or(left, right) =>
        Or(toDNF(left), toDNF(right))
      case And(left, right) =>
        var ans: Expression = null
        val tmp_left = toDNF(left)
        val tmp_right = toDNF(right)
        tmp_left match {
          case Or(l, r) =>
            ans = Or(And(l, tmp_right), And(r, tmp_right))
          case _ =>
        }
        tmp_right match {
          case Or(l, r) =>
            if (ans == null) ans = Or(And(tmp_left, l), And(tmp_left, r))
          case _ =>
        }
        if (ans == null) And(tmp_left, tmp_right)
        else toDNF(ans)
      case exp => exp
    }
  }

  def toCNF(condition: Expression): Expression = {
    condition match {
      case And(left, right) =>
        And(toCNF(left), toCNF(right))
      case Or(left, right) =>
        var ans: Expression = null
        val tmp_left = toCNF(left)
        val tmp_right = toCNF(right)
        tmp_left match {
          case And(l, r) =>
            ans = And(Or(l, tmp_right), Or(r, tmp_right))
          case _ =>
        }
        tmp_right match {
          case And(l, r) =>
            if (ans == null) ans = And(Or(tmp_left, l), Or(tmp_left, r))
          case _ =>
        }
        if (ans == null) Or(tmp_left, tmp_right)
        else toCNF(ans)
      case exp => exp
    }
  }
  def dnfExtract(expression: Expression): Seq[Expression] = {
    expression match {
      case Or(left, right) =>
        dnfExtract(left) ++ dnfExtract(right)
      case And(left @ And(l2, r2), right) =>
        dnfExtract(And(l2, And(r2, right)))
      case other =>
        other :: Nil
    }
  }

  def cnfExtract(expression: Expression): Seq[Expression] = {
    expression match {
      case And(left, right) =>
        cnfExtract(left) ++ cnfExtract(right)
      case Or(left @ Or(l2, r2), right) =>
        cnfExtract(Or(l2, Or(r2, right)))
      case other =>
        other :: Nil
    }
  }

  def splitDNFPredicates(condition: Expression) = dnfExtract(toDNF(condition))

  def splitCNFPredicates(condition: Expression) = cnfExtract(toCNF(condition))


  def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  def splitDisjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

}
