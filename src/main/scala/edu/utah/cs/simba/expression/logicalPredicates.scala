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

package edu.utah.cs.simba.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryOperator, Expression, ImplicitCastInputTypes, Predicate, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
  * Created by dongx on 11/13/2016.
  */
object LogicalPredicateHelper {
  def hasKNN(x: Expression): Boolean = {
    x match {
      case now@And(_, _) => now.hasKNN
      case now@Or(_, _) => now.hasKNN
      case now@Not(_) => now.hasKNN
      case InKNN(_, _, _) => true
      case _ => false
    }
  }
}

case class Not(child: Expression) extends UnaryExpression with Predicate with ImplicitCastInputTypes {
  val hasKNN: Boolean = LogicalPredicateHelper.hasKNN(child)

  override def toString: String = s"NOT $child"

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  protected override def nullSafeEval(input: Any): Any = !input.asInstanceOf[Boolean]

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"!($c)")
  }
}

case class And(left: Expression, right: Expression) extends BinaryOperator with Predicate {
  val hasKNN: Boolean = LogicalPredicateHelper.hasKNN(left) || LogicalPredicateHelper.hasKNN(right)

  override def inputType = BooleanType
  override def symbol: String = "&&"

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    if (input1 == false) {
      false
    } else {
      val input2 = right.eval(input)
      if (input2 == false) {
        false
      } else {
        if (input1 != null && input2 != null) {
          true
        } else {
          null
        }
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)

    // The result should be `false`, if any of them is `false` whenever the other is null or not.
    s"""
      ${eval1.code}
      boolean ${ev.isNull} = false;
      boolean ${ev.value} = false;

      if (!${eval1.isNull} && !${eval1.value}) {
      } else {
        ${eval2.code}
        if (!${eval2.isNull} && !${eval2.value}) {
        } else if (!${eval1.isNull} && !${eval2.isNull}) {
          ${ev.value} = true;
        } else {
          ${ev.isNull} = true;
        }
      }
     """
  }
}


case class Or(left: Expression, right: Expression) extends BinaryOperator with Predicate {
  val hasKNN: Boolean = LogicalPredicateHelper.hasKNN(left) || LogicalPredicateHelper.hasKNN(right)

  override def inputType = BooleanType
  override def symbol: String = "||"

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    if (input1 == true) {
      true
    } else {
      val input2 = right.eval(input)
      if (input2 == true) {
        true
      } else {
        if (input1 != null && input2 != null) {
          false
        } else {
          null
        }
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)

    // The result should be `true`, if any of them is `true` whenever the other is null or not.
    s"""
      ${eval1.code}
      boolean ${ev.isNull} = false;
      boolean ${ev.value} = true;

      if (!${eval1.isNull} && ${eval1.value}) {
      } else {
        ${eval2.code}
        if (!${eval2.isNull} && ${eval2.value}) {
        } else if (!${eval1.isNull} && !${eval2.isNull}) {
          ${ev.value} = false;
        } else {
          ${ev.isNull} = true;
        }
      }
     """
  }
}
