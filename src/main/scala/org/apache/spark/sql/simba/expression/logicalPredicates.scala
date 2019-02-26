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

package org.apache.spark.sql.simba.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryOperator, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant, Predicate, UnaryExpression}
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType}

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

@ExpressionDescription(
  usage = "_FUNC_ expr - Logical not.")
case class Not(child: Expression)
  extends UnaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  val hasKNN: Boolean = LogicalPredicateHelper.hasKNN(child)

  override def toString: String = s"NOT $child"

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  protected override def nullSafeEval(input: Any): Any = !input.asInstanceOf[Boolean]

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"!($c)")
  }

  override def sql: String = s"(NOT ${child.sql})"
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Logical AND.")
case class And(left: Expression, right: Expression) extends BinaryOperator with Predicate {

  val hasKNN: Boolean = LogicalPredicateHelper.hasKNN(left) || LogicalPredicateHelper.hasKNN(right)

  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "&&"

  override def sqlOperator: String = "AND"

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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)

    // The result should be `false`, if any of them is `false` whenever the other is null or not.
    if (!left.nullable && !right.nullable) {
      ev.copy(code = s"""
        ${eval1.code}
        boolean ${ev.value} = false;
        if (${eval1.value}) {
          ${eval2.code}
          ${ev.value} = ${eval2.value};
        }""", isNull = "false")
    } else {
      ev.copy(code = s"""
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
      """)
    }
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Logical OR.")
case class Or(left: Expression, right: Expression) extends BinaryOperator with Predicate {

  val hasKNN: Boolean = LogicalPredicateHelper.hasKNN(left) || LogicalPredicateHelper.hasKNN(right)

  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "||"

  override def sqlOperator: String = "OR"

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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)

    // The result should be `true`, if any of them is `true` whenever the other is null or not.
    if (!left.nullable && !right.nullable) {
      ev.isNull = "false"
      ev.copy(code = s"""
        ${eval1.code}
        boolean ${ev.value} = true;
        if (!${eval1.value}) {
          ${eval2.code}
          ${ev.value} = ${eval2.value};
        }""", isNull = "false")
    } else {
      ev.copy(code = s"""
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
      """)
    }
  }
}