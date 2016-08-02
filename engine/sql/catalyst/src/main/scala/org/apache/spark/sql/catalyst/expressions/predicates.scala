/*
 *  Copyright 2016 by Simba Project                                   
 *                                                                            
 *  Licensed under the Apache License, Version 2.0 (the "License");           
 *  you may not use this file except in compliance with the License.          
 *  You may obtain a copy of the License at                                   
 *                                                                            
 *    http://www.apache.org/licenses/LICENSE-2.0                              
 *                                                                            
 *  Unless required by applicable law or agreed to in writing, software       
 *  distributed under the License is distributed on an "AS IS" BASIS,         
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 *  See the License for the specific language governing permissions and       
 *  limitations under the License.                                            
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, CodegenFallback, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.{NumberConverter, TypeUtils}
import org.apache.spark.sql.spatial.{MBR, Point, Shape, Circle}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


object InterpretedPredicate {
  def create(expression: Expression, inputSchema: Seq[Attribute]): (InternalRow => Boolean) =
    create(BindReferences.bindReference(expression, inputSchema))

  def create(expression: Expression): (InternalRow => Boolean) = {
    expression.foreach {
      case n: Nondeterministic => n.setInitialValues()
      case _ =>
    }
    (r: InternalRow) => expression.eval(r).asInstanceOf[Boolean]
  }
}


/**
 * An [[Expression]] that returns a boolean value.
 */
trait Predicate extends Expression {
  override def dataType: DataType = BooleanType
}


trait PredicateHelper {
  protected def toDNF(condition: Expression): Expression = {
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

  protected def toCNF(condition: Expression): Expression = {
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
  protected def dnfExtract(expression: Expression): Seq[Expression] = {
    expression match {
      case Or(left, right) =>
        dnfExtract(left) ++ dnfExtract(right)
      case And(left @ And(l2, r2), right) =>
        dnfExtract(And(l2, And(r2, right)))
      case other =>
        other :: Nil
    }
  }

  protected def cnfExtract(expression: Expression): Seq[Expression] = {
    expression match {
      case And(left, right) =>
        cnfExtract(left) ++ cnfExtract(right)
      case Or(left @ Or(l2, r2), right) =>
        cnfExtract(Or(l2, Or(r2, right)))
      case other =>
        other :: Nil
    }
  }

  protected def splitDNFPredicates(condition: Expression) = dnfExtract(toDNF(condition))

  protected def splitCNFPredicates(condition: Expression) = cnfExtract(toCNF(condition))


  protected def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  protected def splitDisjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  // Substitute any known alias from a map.
  protected def replaceAlias(
      condition: Expression,
      aliases: AttributeMap[Expression]): Expression = {
    condition.transform {
      case a: Attribute => aliases.getOrElse(a, a)
    }
  }

  /**
   * Returns true if `expr` can be evaluated using only the output of `plan`.  This method
   * can be used to determine when it is acceptable to move expression evaluation within a query
   * plan.
   *
   * For example consider a join between two relations R(a, b) and S(c, d).
   *
   * `canEvaluate(EqualTo(a,b), R)` returns `true` where as `canEvaluate(EqualTo(a,c), R)` returns
   * `false`.
   */
  protected def canEvaluate(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.subsetOf(plan.outputSet)
}


case class Not(child: Expression)
  extends UnaryExpression with Predicate with ImplicitCastInputTypes {

  override def toString: String = s"NOT $child"

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  protected override def nullSafeEval(input: Any): Any = !input.asInstanceOf[Boolean]

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"!($c)")
  }
}


/**
 * Evaluates to `true` if `list` contains `value`.
 */
case class In(value: Expression, list: Seq[Expression]) extends Predicate
    with ImplicitCastInputTypes {

  require(list != null, "list should not be null")

  override def inputTypes: Seq[AbstractDataType] = value.dataType +: list.map(_.dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (list.exists(l => l.dataType != value.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        "Arguments must be same type")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def children: Seq[Expression] = value +: list

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  override def toString: String = s"$value IN ${list.mkString("(", ",", ")")}"

  override def eval(input: InternalRow): Any = {
    val evaluatedValue = value.eval(input)
    if (evaluatedValue == null) {
      null
    } else {
      var hasNull = false
      list.foreach { e =>
        val v = e.eval(input)
        if (v == evaluatedValue) {
          return true
        } else if (v == null) {
          hasNull = true
        }
      }
      if (hasNull) {
        null
      } else {
        false
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val valueGen = value.gen(ctx)
    val listGen = list.map(_.gen(ctx))
    val listCode = listGen.map(x =>
      s"""
        if (!${ev.value}) {
          ${x.code}
          if (${x.isNull}) {
            ${ev.isNull} = true;
          } else if (${ctx.genEqual(value.dataType, valueGen.value, x.value)}) {
            ${ev.isNull} = false;
            ${ev.value} = true;
          }
        }
       """).mkString("\n")
    s"""
      ${valueGen.code}
      boolean ${ev.value} = false;
      boolean ${ev.isNull} = ${valueGen.isNull};
      if (!${ev.isNull}) {
        $listCode
      }
    """
  }
}

case class InRange(point: Expression,
                   point_low: Expression,
                   point_high: Expression) extends Predicate with CodegenFallback {

  override def children: Seq[Expression] = Seq(point, point_low, point_high)

  override def nullable: Boolean = false

  override def toString: String = s" **($point) IN Rectangle ($point_low) - ($point_high)**  "

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = {
    val eval_point = point.eval(input).asInstanceOf[Point]
    val eval_low = point_low.asInstanceOf[Literal].value.asInstanceOf[Point]
    val eval_high = point_high.asInstanceOf[Literal].value.asInstanceOf[Point]
    require(eval_point.coord.length == eval_low.coord.length &&
      eval_point.coord.length == eval_high.coord.length)
    val mbr = MBR(eval_low, eval_high)
    mbr.contains(eval_point)
  }
}

case class InCircleRange(point: Expression,
                         target: Expression,
                         r: Literal) extends Predicate with CodegenFallback {
  require(r.dataType.isInstanceOf[NumericType])

  override def children: Seq[Expression] = Seq(point, target, r)

  override def nullable: Boolean = false

  override def toString: String = s" **($point) IN CIRCLERANGE ($target) within  ($r)**  "

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = {
    val eval_point = point.eval(input).asInstanceOf[Point]
    val eval_target = target.eval(input).asInstanceOf[Point]
    require(eval_point.coord.length == eval_target.coord.length)
    val eval_r = NumberConverter.literalToDouble(r)
    Circle(eval_target, eval_r).contains(eval_point)
  }
}

case class InKNN(point: Expression,
                 target: Expression,
                 k: Literal) extends Predicate with CodegenFallback {
  require(k.dataType.isInstanceOf[IntegralType])

  override def hasSpatial: Boolean = true

  override def children: Seq[Expression] = Seq(point, target, k)

  override def nullable: Boolean = false

  override def toString: String = s" **($point) IN KNN ($target) within ($k)"

  // XX Tricky hack
  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = true
}

case class Intersects(left: Expression, right: Expression)
    extends BinaryExpression with Predicate with CodegenFallback {
  override def toString: String = s"**($left) INTERSECTS ($right)"

  override def nullable: Boolean = left.nullable || right.nullable

  override def eval(input: InternalRow): Boolean = {
    val leftEval = left.eval(input)
    if (leftEval == null) false
    else {
      val rightEval = right.eval(input)
      if (rightEval == null) false
      else leftEval.asInstanceOf[Shape].intersects(rightEval.asInstanceOf[Shape])
    }
  }
}


/**
 * Optimized version of In clause, when all filter values of In clause are
 * static.
 */
case class InSet(child: Expression, hset: Set[Any]) extends UnaryExpression with Predicate {

  require(hset != null, "hset could not be null")

  override def toString: String = s"$child INSET ${hset.mkString("(", ",", ")")}"

  @transient private[this] lazy val hasNull: Boolean = hset.contains(null)

  override def nullable: Boolean = child.nullable || hasNull

  protected override def nullSafeEval(value: Any): Any = {
    if (hset.contains(value)) {
      true
    } else if (hasNull) {
      null
    } else {
      false
    }
  }

  def getHSet: Set[Any] = hset

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val setName = classOf[Set[Any]].getName
    val InSetName = classOf[InSet].getName
    val childGen = child.gen(ctx)
    ctx.references += this
    val hsetTerm = ctx.freshName("hset")
    val hasNullTerm = ctx.freshName("hasNull")
    ctx.addMutableState(setName, hsetTerm,
      s"$hsetTerm = (($InSetName)expressions[${ctx.references.size - 1}]).getHSet();")
    ctx.addMutableState("boolean", hasNullTerm, s"$hasNullTerm = $hsetTerm.contains(null);")
    s"""
      ${childGen.code}
      boolean ${ev.isNull} = ${childGen.isNull};
      boolean ${ev.value} = false;
      if (!${ev.isNull}) {
        ${ev.value} = $hsetTerm.contains(${childGen.value});
        if (!${ev.value} && $hasNullTerm) {
          ${ev.isNull} = true;
        }
      }
     """
  }
}

case class And(left: Expression, right: Expression) extends BinaryOperator with Predicate {

  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "&&"

  override def hasSpatial: Boolean = left.hasSpatial || right.hasSpatial

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

  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "||"

  override def hasSpatial: Boolean = left.hasSpatial || right.hasSpatial

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


abstract class BinaryComparison extends BinaryOperator with Predicate {

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (ctx.isPrimitiveType(left.dataType)
        && left.dataType != BooleanType // java boolean doesn't support > or < operator
        && left.dataType != FloatType
        && left.dataType != DoubleType) {
      // faster version
      defineCodeGen(ctx, ev, (c1, c2) => s"$c1 $symbol $c2")
    } else {
      defineCodeGen(ctx, ev, (c1, c2) => s"${ctx.genComp(left.dataType, c1, c2)} $symbol 0")
    }
  }
}


private[sql] object BinaryComparison {
  def unapply(e: BinaryComparison): Option[(Expression, Expression)] = Some((e.left, e.right))
}


/** An extractor that matches both standard 3VL equality and null-safe equality. */
private[sql] object Equality {
  def unapply(e: BinaryComparison): Option[(Expression, Expression)] = e match {
    case EqualTo(l, r) => Some((l, r))
    case EqualNullSafe(l, r) => Some((l, r))
    case _ => None
  }
}


case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = AnyDataType

  override def symbol: String = "="

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    if (left.dataType == FloatType) {
      Utils.nanSafeCompareFloats(input1.asInstanceOf[Float], input2.asInstanceOf[Float]) == 0
    } else if (left.dataType == DoubleType) {
      Utils.nanSafeCompareDoubles(input1.asInstanceOf[Double], input2.asInstanceOf[Double]) == 0
    } else if (left.dataType != BinaryType) {
      input1 == input2
    } else {
      java.util.Arrays.equals(input1.asInstanceOf[Array[Byte]], input2.asInstanceOf[Array[Byte]])
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => ctx.genEqual(left.dataType, c1, c2))
  }
}


case class EqualNullSafe(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = AnyDataType

  override def symbol: String = "<=>"

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    val input2 = right.eval(input)
    if (input1 == null && input2 == null) {
      true
    } else if (input1 == null || input2 == null) {
      false
    } else {
      if (left.dataType == FloatType) {
        Utils.nanSafeCompareFloats(input1.asInstanceOf[Float], input2.asInstanceOf[Float]) == 0
      } else if (left.dataType == DoubleType) {
        Utils.nanSafeCompareDoubles(input1.asInstanceOf[Double], input2.asInstanceOf[Double]) == 0
      } else if (left.dataType != BinaryType) {
        input1 == input2
      } else {
        java.util.Arrays.equals(input1.asInstanceOf[Array[Byte]], input2.asInstanceOf[Array[Byte]])
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val equalCode = ctx.genEqual(left.dataType, eval1.value, eval2.value)
    ev.isNull = "false"
    eval1.code + eval2.code + s"""
        boolean ${ev.value} = (${eval1.isNull} && ${eval2.isNull}) ||
           (!${eval1.isNull} && $equalCode);
      """
  }
}


case class LessThan(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = "<"

  private lazy val ordering = TypeUtils.getInterpretedOrdering(left.dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.lt(input1, input2)
}


case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = "<="

  private lazy val ordering = TypeUtils.getInterpretedOrdering(left.dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.lteq(input1, input2)
}


case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = ">"

  private lazy val ordering = TypeUtils.getInterpretedOrdering(left.dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.gt(input1, input2)
}


case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def symbol: String = ">="

  private lazy val ordering = TypeUtils.getInterpretedOrdering(left.dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = ordering.gteq(input1, input2)
}
