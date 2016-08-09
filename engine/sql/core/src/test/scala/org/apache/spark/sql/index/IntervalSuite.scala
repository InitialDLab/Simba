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

package org.apache.spark.sql.index

import org.apache.spark.sql.catalyst.util.NumberConverter
import org.scalatest.FunSuite
import org.apache.spark.sql.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._



/**
  * Created by Zhihao Bai on 16-8-3.
  */
class IntervalSuite extends FunSuite{
  def equal(A: Interval, B: Interval): Boolean = {
    A.min == B.min && A.max == B.max
  }

  test("Interval: isNull"){
    val i = new Interval((0.0, false), (0.0, true))
    assert(i.isNull)
  }

  test("Interval: intersect"){
    val I = new Interval((-1.0,  true),  (1.0, true))
    val J = new Interval((0.0, false), (2.0, false))
    val K1 = new Interval((1.0, true), (3.0, false))
    val K2 = new Interval((1.0, false), (3.0, false))

    assert(equal(I.intersect(J), new Interval((0.0, false),  (1.0, true))))
    assert(equal(I.intersect(K1), new Interval((1.0, true),  (1.0, true))))
    assert(equal(I.intersect(K2), new Interval((1.0, false),  (1.0, true))))
  }

  test("Interval: getLeafInterval"){
    val left = new Alias(Literal.apply(0), "zero")()
    val right = Literal.apply(10)

    val eq = new EqualTo(left, right)
    assert(equal(Interval.getLeafInterval(eq)._1,
      new Interval((10.0, true), (10.0, true))))

    val lt = new LessThan(left, right)
    assert(equal(Interval.getLeafInterval(lt)._1,
      new Interval((Double.MinValue, false), (10.0, false))))

    val leq = new LessThanOrEqual(left, right)
    assert(equal(Interval.getLeafInterval(leq)._1,
      new Interval((Double.MinValue, false), (10.0, true))))

    val gt = new GreaterThan(left, right)
    assert(equal(Interval.getLeafInterval(gt)._1,
      new Interval((10.0, false), (Double.MaxValue, false))))

    val geq = new GreaterThanOrEqual(left, right)
    assert(equal(Interval.getLeafInterval(geq)._1,
      new Interval((10.0, true), (Double.MaxValue, false))))
  }

  test("Interval: conditionToInterval"){
    val zero = new Alias(Literal.apply(0), "zero")()
    val ten = new Alias(Literal.apply(10),  "ten")()

    val lt = new LessThan(zero, Literal.apply(10))
    val gt = new GreaterThan(ten, Literal.apply(0))

    val and = new And(lt, gt)
    var column = ten.toAttribute :: zero.toAttribute :: Nil

    val r = Interval.conditionToInterval(and, column, 2)._1
    assert(equal(r(0), new Interval((0.0, false), (Double.MaxValue,false))))
    assert(equal(r(1), new Interval((Double.MinValue, false), (10.0,false))))
  }

  test("Interval: getBoundNumberForInterval"){
    def equalArray(x: Seq[Int], y: Seq[Int]): Boolean = {
      if(x.length != y.length)
        false
      else{
        var eq = true
        for(i <- 0 until x.length)
          eq = eq && (x(0) == y(0))
        eq
      }
    }

    val I = new Interval(2.5, 4.5)
    val J = new Interval(5.5, 6.5)
    val A = Array(1.0, 2.0, 3.0, 4.0, 5.0)

    val x = Interval.getBoundNumberForInterval(I, A).sorted
    assert(equalArray(x, Array(2, 3, 4, 5)))

    val y = Interval.getBoundNumberForInterval(J, A).sorted
    assert(equalArray(y, Array(5)))
  }
}
