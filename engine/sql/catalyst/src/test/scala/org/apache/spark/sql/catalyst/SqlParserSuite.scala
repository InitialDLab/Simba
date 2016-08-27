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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count, HyperLogLogPlusPlus}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, Ascending, Attribute, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, CaseKeyWhen, CaseWhen, Cast, Descending, Divide, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, InCircleRange, InKNN, InRange, IsNotNull, IsNull, LessThan, LessThanOrEqual, Like, Literal, Multiply, Not, Or, PointWrapperExpression, RLike, Remainder, SortOrder, Subtract, UnaryMinus, aggregate}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.spatial.Point
import org.apache.spark.sql.types.{BooleanType, IntegerType, NullType, StringType}
import org.apache.spark.unsafe.types.CalendarInterval

private[sql] case class TestCommand(cmd: String) extends LogicalPlan with Command {
  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}

private[sql] class SuperLongKeywordTestParser extends AbstractSparkSQLParser {
  protected val EXECUTE = Keyword("THISISASUPERLONGKEYWORDTEST")

  override protected lazy val start: Parser[LogicalPlan] = set

  private lazy val set: Parser[LogicalPlan] =
    EXECUTE ~> ident ^^ {
      case fileName => TestCommand(fileName)
    }
}

private[sql] class CaseInsensitiveTestParser extends AbstractSparkSQLParser {
  protected val EXECUTE = Keyword("EXECUTE")

  override protected lazy val start: Parser[LogicalPlan] = set

  private lazy val set: Parser[LogicalPlan] =
    EXECUTE ~> ident ^^ {
      case fileName => TestCommand(fileName)
    }
}

class SqlParserSuite extends PlanTest {

  test("test long keyword") {
    val parser = new SuperLongKeywordTestParser
    assert(TestCommand("NotRealCommand") ===
      parser.parse("ThisIsASuperLongKeyWordTest NotRealCommand"))
  }

  test("test case insensitive") {
    val parser = new CaseInsensitiveTestParser
    assert(TestCommand("NotRealCommand") === parser.parse("EXECUTE NotRealCommand"))
    assert(TestCommand("NotRealCommand") === parser.parse("execute NotRealCommand"))
    assert(TestCommand("NotRealCommand") === parser.parse("exEcute NotRealCommand"))
  }

  test("test NOT operator with comparison operations") {
    val parsed = SqlParser.parse("SELECT NOT TRUE > TRUE")
    val expected = Project(
      UnresolvedAlias(
        Not(
          GreaterThan(Literal(true), Literal(true)))
      ) :: Nil,
      OneRowRelation)
    comparePlans(parsed, expected)
  }

  test("support hive interval literal") {
    def checkInterval(sql: String, result: CalendarInterval): Unit = {
      val parsed = SqlParser.parse(sql)
      val expected = Project(
        UnresolvedAlias(
          Literal(result)
        ) :: Nil,
        OneRowRelation)
      comparePlans(parsed, expected)
    }

    def checkYearMonth(lit: String): Unit = {
      checkInterval(
        s"SELECT INTERVAL '$lit' YEAR TO MONTH",
        CalendarInterval.fromYearMonthString(lit))
    }

    def checkDayTime(lit: String): Unit = {
      checkInterval(
        s"SELECT INTERVAL '$lit' DAY TO SECOND",
        CalendarInterval.fromDayTimeString(lit))
    }

    def checkSingleUnit(lit: String, unit: String): Unit = {
      checkInterval(
        s"SELECT INTERVAL '$lit' $unit",
        CalendarInterval.fromSingleUnitString(unit, lit))
    }

    checkYearMonth("123-10")
    checkYearMonth("496-0")
    checkYearMonth("-2-3")
    checkYearMonth("-123-0")

    checkDayTime("99 11:22:33.123456789")
    checkDayTime("-99 11:22:33.123456789")
    checkDayTime("10 9:8:7.123456789")
    checkDayTime("1 0:0:0")
    checkDayTime("-1 0:0:0")
    checkDayTime("1 0:0:1")

    for (unit <- Seq("year", "month", "day", "hour", "minute", "second")) {
      checkSingleUnit("7", unit)
      checkSingleUnit("-7", unit)
      checkSingleUnit("0", unit)
    }

    checkSingleUnit("13.123456789", "second")
    checkSingleUnit("-13.123456789", "second")
  }

  test("support scientific notation") {
    def assertRight(input: String, output: Double): Unit = {
      val parsed = SqlParser.parse("SELECT " + input)
      val expected = Project(
        UnresolvedAlias(
          Literal(output)
        ) :: Nil,
        OneRowRelation)
      comparePlans(parsed, expected)
    }

    assertRight("9.0e1", 90)
    assertRight(".9e+2", 90)
    assertRight("0.9e+2", 90)
    assertRight("900e-1", 90)
    assertRight("900.0E-1", 90)
    assertRight("9.e+1", 90)

    intercept[RuntimeException](SqlParser.parse("SELECT .e3"))
  }

  /* Following tests are written by Zhihao Bai*/

  def expr(ident: String) = UnresolvedAttribute.quoted(ident)
  def proj(ident: String) = Project(UnresolvedAlias(expr(ident)) :: Nil,OneRowRelation)
  def rela(ident: String) = UnresolvedRelation(TableIdentifier(ident, None), None)
  def test(sql: String, expected: LogicalPlan): Unit = {
    val parsed = SqlParser.parse(sql)
    comparePlans(parsed, expected)
  }

  test("test start1 with comparison operations") {
    test("SELECT x", proj("x"))
    test("(SELECT x) UNION ALL (SELECT y)", Union(proj("x"), proj("y")))
    test("(SELECT x) INTERSECT (SELECT y)", Intersect(proj("x"), proj("y")))
    test("(SELECT x) EXCEPT (SELECT y)", Except(proj("x"), proj("y")))
    test("(SELECT x) UNION (SELECT y)", Distinct(Union(proj("x"), proj("y"))))
  }

  test("test select with comparison operations") {
    val sql = "SELECT " +
      "DISTINCT " +
      "a AS b " +
      "FROM c " +
      "WHERE d " +
      "GROUP BY e " +
      "HAVING f " +
      "ORDER BY g " +
      "LIMIT h"
    val expected =
      Limit(expr("h"),
        Sort(Seq(SortOrder(expr("g"), Ascending)), true,
          Filter(expr("f"),
            Distinct(
              Aggregate(Seq(expr("e")),
                Seq(UnresolvedAlias(Alias(expr("a"), "b")())),
                Filter(expr("d"),
                  rela("c")
                )
              )
            )
          )
        )
      )
    test(sql, expected)
  }

  test("test insert with comparison operations") {
    val expectedOverwrite = InsertIntoTable(
      rela("t"),
      Map.empty[String, Option[String]],
      proj("x"),
      true,
      false)
    test("INSERT OVERWRITE TABLE t SELECT x", expectedOverwrite)

    val expectedInto = InsertIntoTable(
      rela("t"),
      Map.empty[String, Option[String]],
      proj("x"),
      false,
      false)
    test("INSERT INTO TABLE t SELECT x", expectedInto)
  }

  test("test cte with comparison operations") {
    val sql = "WITH x AS (SELECT y) SELECT z"
    val expected = With(
      proj("z"),
      Map(("x", Subquery("x", proj("y"))))
    )
    test(sql, expected)
  }

  test("test projection with comparison operations") {
    val sql = "SELECT x AS y"
    val expected = Project(UnresolvedAlias(Alias(expr("x"), "y")()) :: Nil, OneRowRelation)
    test(sql, expected)
  }

  def testRelations(sql: String, rel: LogicalPlan): Unit = {
    val expected = Project(UnresolvedAlias(expr("x")) :: Nil, rel)
    test(sql, expected)
  }

  test("test relations with comparison operations") {
    testRelations("SELECT x FROM y, z",
      Join(rela("y"), rela("z"),
        Inner,
        None)
    )
  }

  test("test relationFactor with comparison operations") {
    testRelations("SELECT x FROM y",
      UnresolvedRelation(TableIdentifier("y", None), None)
    )

    testRelations("SELECT x FROM y AS z",
      UnresolvedRelation(TableIdentifier("y", None), Some("z"))
    )
  }

  test("test joinedRelation with comparison operations") {
    testRelations("SELECT x FROM y JOIN z1 on w1 JOIN z2 ON w2",
      Join(Join(rela("y"), rela("z1"), Inner, Some(expr("w1"))),
        rela("z2"),
        Inner,
        Some(expr("w2"))
      )
    )
  }

  test("test joinConditions with comparison operations") {
    testRelations("SELECT x FROM y JOIN z ON POINT(0, 0) IN KNN (POINT(1, 1), 2)",
      Join(rela("y"), rela("z"), Inner,
        Some(InKNN(PointWrapperExpression(Seq(Literal(0), Literal(0))),
          PointWrapperExpression(Seq(Literal(1), Literal(1))),
          Literal(2))))
    )

    testRelations("SELECT x FROM y JOIN z ON POINT(0, 0) IN CIRCLERANGE (POINT(1, 1), 2)",
      Join(rela("y"), rela("z"), Inner,
        Some(InCircleRange(PointWrapperExpression(Seq(Literal(0), Literal(0))),
          PointWrapperExpression(Seq(Literal(1), Literal(1))),
          Literal(2))))
    )

    testRelations("SELECT x FROM y JOIN z ON w",
      Join(rela("y"), rela("z"), Inner,
        Some(expr("w")))
    )
  }

  test("test joinType with comparison operations") {
    testRelations("SELECT x FROM y INNER JOIN z",
      Join(rela("y"), rela("z"),
        Inner,
        None)
    )

    testRelations("SELECT x FROM y LEFT SEMI JOIN z",
      Join(rela("y"), rela("z"),
        LeftSemi,
        None)
    )

    testRelations("SELECT x FROM y LEFT OUTER JOIN z",
      Join(rela("y"), rela("z"),
        LeftOuter,
        None)
    )

    testRelations("SELECT x FROM y RIGHT OUTER JOIN z",
      Join(rela("y"), rela("z"),
        RightOuter,
        None)
    )

    testRelations("SELECT x FROM y FULL OUTER JOIN z",
      Join(rela("y"), rela("z"),
        FullOuter,
        None)
    )

    testRelations("SELECT x FROM y KNN JOIN z",
      Join(rela("y"), rela("z"),
        KNNJoin,
        None)
    )

    testRelations("SELECT x FROM y ZKNN JOIN z",
      Join(rela("y"), rela("z"),
        ZKNNJoin,
        None)
    )

    testRelations("SELECT x FROM y DISTANCE JOIN z",
      Join(rela("y"), rela("z"),
        DistanceJoin,
        None)
    )
  }

  def testSort(sql: String, orders: Seq[SortOrder], orderOrSort: Boolean): Unit = {
    val expected = Sort(
      orders,
      orderOrSort,
      proj("x")
    )
    test(sql, expected)
  }

  test("test sortType with comparison operations") {
    testSort("SELECT x ORDER BY y ASC",
      Seq(SortOrder(expr("y"), Ascending)),
      true
    )

    testSort("SELECT x SORT BY y ASC",
      Seq(SortOrder(expr("y"), Ascending)),
      false
    )
  }

  test("test ordering with comparison operations") {
    testSort("SELECT x ORDER BY w, y ASC, z DESC",
      Seq(SortOrder(expr("w"), Ascending),
        SortOrder(expr("y"), Ascending),
        SortOrder(expr("z"),Descending)),
      true
    )
  }

  test("test direction with comparison operations") {
    testSort("SELECT x ORDER BY y ASC",
      Seq(SortOrder(expr("y"), Ascending)),
      true
    )

    testSort("SELECT x ORDER BY y DESC",
      Seq(SortOrder(expr("y"), Descending)),
      true
    )
  }

  def testExpr(sql: String, e: Expression): Unit = {
    val expected = Project(
      UnresolvedAlias(
        e
      ) :: Nil,
      OneRowRelation
    )
    test(sql, expected)
  }

  test("test orExpression with comparison operations") {
    testExpr("SELECT x OR y",
      Or(expr("x"), expr("y"))
    )
  }

  test("test andExpression with comparison operations") {
    testExpr("SELECT x AND y",
      And(expr("x"), expr("y"))
    )
  }

  test("test notExpression with comparison operations") {
    testExpr("SELECT NOT x",
      Not(expr("x"))
    )
  }

  test("test comparisonExpression with comparison operations") {
    val parsed1 = SqlParser.parse("SELECT POINT(0, 1) IN RANGE (POINT(0, 0), POINT(1, 1) )")
    val expected1 = Project(
      UnresolvedAlias(
        InRange(PointWrapperExpression(Seq(Literal(0), Literal(1))),
          Literal(Point(Array(0, 0))),
          Literal(Point(Array(1, 1))))
      ) :: Nil,
      OneRowRelation
    )
    assert(parsed1.toString == expected1.toString)

    val parsed2 = SqlParser.parse("SELECT POINT(0, 1) IN KNN (POINT(0, 0), 1 )")
    val expected2 = Project(
      UnresolvedAlias(
        InKNN(PointWrapperExpression(Seq(Literal(0), Literal(1))),
          Literal(Point(Array(0, 0))),
          Literal(1))
      ) :: Nil,
      OneRowRelation
    )
    assert(parsed2.toString == expected2.toString)

    val parsed3 = SqlParser.parse("SELECT POINT(0, 1) IN CIRCLERANGE (POINT(0, 0), 1 )")
    val expected3 = Project(
      UnresolvedAlias(
        InCircleRange(PointWrapperExpression(Seq(Literal(0), Literal(1))),
          Literal(Point(Array(0, 0))),
          Literal(1))
      ) :: Nil,
      OneRowRelation
    )
    assert(parsed3.toString == expected3.toString)

    testExpr("SELECT x = y",
      EqualTo(expr("x"), expr("y"))
    )

    testExpr("SELECT x < y",
      LessThan(expr("x"), expr("y"))
    )

    testExpr("SELECT x <= y",
      LessThanOrEqual(expr("x"), expr("y"))
    )

    testExpr("SELECT x > y",
      GreaterThan(expr("x"), expr("y"))
    )

    testExpr("SELECT x >= y",
      GreaterThanOrEqual(expr("x"), expr("y"))
    )

    testExpr("SELECT x != y",
      Not(EqualTo(expr("x"), expr("y")))
    )

    testExpr("SELECT x <> y",
      Not(EqualTo(expr("x"), expr("y")))
    )

    testExpr("SELECT x <=> y",
      EqualNullSafe(expr("x"), expr("y"))
    )

    testExpr("SELECT x NOT BETWEEN y AND z",
      Not(And(
        GreaterThanOrEqual(expr("x"), expr("y")),
        LessThanOrEqual(expr("x"), expr("z"))
      ))
    )

    testExpr("SELECT x RLIKE y",
      RLike(expr("x"), expr("y"))
    )

    testExpr("SELECT x REGEXP y",
      RLike(expr("x"), expr("y"))
    )

    testExpr("SELECT x LIKE y",
      Like(expr("x"), expr("y"))
    )

    testExpr("SELECT x NOT LIKE y",
      Not(Like(expr("x"), expr("y")))
    )

    testExpr("SELECT x IN(y)",
      In(expr("x"), Seq(expr("y")))
    )

    testExpr("SELECT x NOT IN(y)",
      Not(In(expr("x"), Seq(expr("y"))))
    )

    testExpr("SELECT x IS NULL",
      IsNull(expr("x"))
    )

    testExpr("SELECT x IS NOT NULL",
      IsNotNull(expr("x"))
    )
  }

  test("test termExpression with comparison operations") {
    testExpr("SELECT x + y",
      Add(expr("x"),expr("y"))
    )

    testExpr("SELECT x - y",
      Subtract(expr("x"),expr("y"))
    )
  }

  test("test productExpression with comparison operations") {
    testExpr("SELECT x * y",
      Multiply(expr("x"),expr("y"))
    )

    testExpr("SELECT x / y",
      Divide(expr("x"),expr("y"))
    )

    testExpr("SELECT x % y",
      Remainder(expr("x"),expr("y"))
    )

    testExpr("SELECT x & y",
      BitwiseAnd(expr("x"),expr("y"))
    )

    testExpr("SELECT x | y",
      BitwiseOr(expr("x"),expr("y"))
    )

    testExpr("SELECT x ^ y",
      BitwiseXor(expr("x"),expr("y"))
    )
  }

  test("test function with comparison operations") {
    val parsed1 = SqlParser.parse("SELECT count(*)")
    val expected1 = Project(
      UnresolvedAlias(
        AggregateExpression(Count(Literal(1)), mode = Complete, isDistinct = false)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed1, expected1)

    val parsed2 = SqlParser.parse("SELECT f(x, y)")
    val expected2 = Project(
      UnresolvedAlias(
        UnresolvedFunction(
          "f",
          Seq(expr("x"),expr("y")),
          isDistinct = false)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed2, expected2)

    val parsed3 = SqlParser.parse("SELECT f(DISTINCT x, y)")
    val expected3 = Project(
      UnresolvedAlias(
        UnresolvedFunction(
          "f",
          Seq(expr("x"),expr("y")),
          isDistinct = true)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed3, expected3)

    val parsed4 = SqlParser.parse("SELECT APPROXIMATE count(DISTINCT x)")
    val expected4 = Project(
      UnresolvedAlias(
        AggregateExpression(
          new HyperLogLogPlusPlus(expr("x")),
          mode = Complete,
          isDistinct = false)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed4, expected4)

    val parsed5 = SqlParser.parse("SELECT APPROXIMATE (0.1) count (DISTINCT x)")
    val expected5 = Project(
      UnresolvedAlias(
        AggregateExpression(
          HyperLogLogPlusPlus(expr("x"), "0.1".toDouble, 0, 0),
          mode = Complete,
          isDistinct = false)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed5, expected5)

    val parsed6 = SqlParser.parse("SELECT CASE WHEN x THEN y ELSE z END")
    val expected6 = Project(
      UnresolvedAlias(
        CaseWhen(Seq(
          expr("x"),
          expr("y"),
          expr("z")))
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed6, expected6)

    val parsed7 = SqlParser.parse("SELECT CASE k WHEN x THEN y ELSE z END")
    val expected7 = Project(
      UnresolvedAlias(
        CaseKeyWhen(
          expr("k"),
          Seq(
            expr("x"),
            expr("y"),
            expr("z")))
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed7, expected7)
  }

  test("test whenThenElse with comparison operations") {
    val parsed = SqlParser.parse("SELECT CASE WHEN x1 THEN y1 WHEN x2 THEN y2 ELSE z END")
    val expected = Project(
      UnresolvedAlias(
        CaseWhen(Seq(
          expr("x1"),
          expr("y1"),
          expr("x2"),
          expr("y2"),
          expr("z")))
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed, expected)
  }

  test("test cast with comparison operations") {
    val parsed = SqlParser.parse("SELECT CAST ( x AS int )")
    val expected = Project(
      UnresolvedAlias(
        Cast(expr("x"), IntegerType)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed, expected)
  }

  test("test literal with comparison operations") {
    val parsedString = SqlParser.parse("SELECT \"string\"")
    val expectedString = Project(
      UnresolvedAlias(
        Literal.create("string", StringType)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsedString, expectedString)

    val parsedNull = SqlParser.parse("SELECT NULL")
    val expectedNull = Project(
      UnresolvedAlias(
        Literal.create(null, NullType)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsedNull, expectedNull)
  }

  test("test booleanLiteral with comparison operations") {
    val parsedTrue = SqlParser.parse("SELECT TRUE")
    val expectedTrue = Project(
      UnresolvedAlias(
        Literal.create(true, BooleanType)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsedTrue, expectedTrue)

    val parsedFalse = SqlParser.parse("SELECT FALSE")
    val expectedFalse = Project(
      UnresolvedAlias(
        Literal.create(false, BooleanType)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsedFalse, expectedFalse)
  }

  test("test numericLiteral with comparison operations") {
    val parsedInt = SqlParser.parse("SELECT 123")
    val expectedInt = Project(
      UnresolvedAlias(
        Literal(123)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsedInt, expectedInt)

    val parsedFloat = SqlParser.parse("SELECT .123")
    val expectedFloat = Project(
      UnresolvedAlias(
        Literal(BigDecimal(0.123))
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsedFloat, expectedFloat)
  }

  test("test pointLiteral with comparison operations") {
    val parsed = SqlParser.parse("SELECT POINT(0, 1, 2)")
    val expected = Project(
      UnresolvedAlias(
        Literal(Point(Array(0, 1, 2)))
      ) :: Nil,
      OneRowRelation
    )
    assert("" + parsed == "" + expected)
//    comparePlans(parsed, expected)
  }

  test("test baseExpression with comparison operations") {
    val parsed1 = SqlParser.parse("SELECT *")
    val expected1 = Project(
      UnresolvedAlias(
        UnresolvedStar(None)
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed1, expected1)

    val parsed2 = SqlParser.parse("SELECT x.y.*")
    val expected2 = Project(
      UnresolvedAlias(
        UnresolvedStar(Some(Seq("x", "y")))
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed2, expected2)
  }

  test("test signedPrimary with comparison operations") {
    val parsedPlus = SqlParser.parse("SELECT +x")
    val expectedPlus = Project(
      UnresolvedAlias(
        expr("x")
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsedPlus, expectedPlus)

    val parsedMinus = SqlParser.parse("SELECT -x")
    val expectedMinus = Project(
      UnresolvedAlias(
        UnaryMinus(expr("x"))
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsedMinus, expectedMinus)
  }

  test("test primary with comparison operations") {
    val parsed2 = SqlParser.parse("SELECT x[y]")
    val expected2 = Project(
      UnresolvedAlias(
        UnresolvedExtractValue(
          expr("x"),
          expr("y")
        )
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed2, expected2)

/*
    var parsed3 = SqlParser.parse("SELECT x.y")
    var expected3 = Project(
      UnresolvedAlias(
        UnresolvedExtractValue(
          expr("x"),
          Literal("y")
        )
      ) :: Nil,
      OneRowRelation
    )
    println(parsed3)
    println(expected3)
    comparePlans(parsed3, expected3)
*/

    val parsed5 = SqlParser.parse("SELECT (x)")
    val expected5 = Project(
      UnresolvedAlias(
        expr("x")
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed5, expected5)

    val parsed9 = SqlParser.parse("SELECT ~x")
    val expected9 = Project(
      UnresolvedAlias(
        BitwiseNot(expr("x"))
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed9, expected9)

    val parsed10 = SqlParser.parse("SELECT x")
    val expected10 = Project(
      UnresolvedAlias(
        expr("x")
      ) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed10, expected10)
  }

  test("test dotExpressionHeader with comparison operations") {
    val parsed = SqlParser.parse("SELECT x.y.z")
    val expected = Project(
      UnresolvedAlias(UnresolvedAttribute(Seq("x", "y", "z"))) :: Nil,
      OneRowRelation
    )
    comparePlans(parsed, expected)
  }

  test("test tableIdentifier with comparison operations") {
    val parsed = SqlParser.parse("SELECT x FROM db.t")
    val expected = Project(
      UnresolvedAlias(expr("x")) :: Nil,
      UnresolvedRelation(TableIdentifier("t", Some("db")))
    )
    comparePlans(parsed, expected)
  }
}
