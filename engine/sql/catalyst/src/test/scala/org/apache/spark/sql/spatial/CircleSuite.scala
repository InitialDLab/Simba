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

package org.apache.spark.sql.spatial

import org.scalatest.FunSuite

/**
  * Created by Zhihao Bai on 16-6-28.
  */
class CircleSuite extends FunSuite{
  test("Circle: intersects and minDist with other Circle"){
    val c1 = Circle(Point(Array(-2.0, 0.0)), 2.0)
    val c2 = Circle(Point(Array(-1.5, 0.0)), 0.5)
    val c3 = Circle(Point(Array(0.0, 1.0)), 1.0)
    val c4 = Circle(Point(Array(1.0, 0.0)), 1.0)
    val c5 = Circle(Point(Array(2.0, 1.0)), 1.0)

    assert(c1.intersects(c2))
    assert(c1.intersects(c3))
    assert(c1.intersects(c4))
    assert(!c1.intersects(c5))

    assert(Math.abs(c1.minDist(c2)) < 1e-8)
    assert(Math.abs(c1.minDist(c3)) < 1e-8)
    assert(Math.abs(c1.minDist(c4)) < 1e-8)
    assert(Math.abs(c1.minDist(c5) - (Math.sqrt(17.0) - 3.0)) < 1e-8)
  }

  val c = Circle(Point(Array(0.0, 0.0)), 1.0)
  test("Circle: intersects and minDist with Point"){
    val p1 = Point(Array(0.5, 0.0))
    val p2 = Point(Array(1.0, 0.0))
    val p3 = Point(Array(1.5, 0.0))

    assert(c.intersects(p1))
    assert(c.intersects(p2))
    assert(!c.intersects(p3))

    assert(Math.abs(c.minDist(p1)) < 1e-8)
    assert(Math.abs(c.minDist(p2)) < 1e-8)
    assert(Math.abs(c.minDist(p3) - 0.5) < 1e-8)
  }
  test("Circle: intersects and minDist with LineSegment"){
    val l1 = LineSegment(Point(Array(0.0, 0.0)), Point(Array(1.0, 1.0)))
    val l2 = LineSegment(Point(Array(1.0, 0.0)), Point(Array(1.0, 1.0)))
    val l3 = LineSegment(Point(Array(2.0, 0.0)), Point(Array(1.0, 1.0)))

    assert(c.intersects(l1))
    assert(c.intersects(l2))
    assert(!c.intersects(l3))

    assert(Math.abs(c.minDist(l1)) < 1e-8)
    assert(Math.abs(c.minDist(l2)) < 1e-8)
    assert(Math.abs(c.minDist(l3) - (Math.sqrt(2.0) - 1.0)) < 1e-8)
  }
  test("Circle: intersects and minDist with MBR"){
    val m1 = MBR(Point(Array(0.0, 0.0)), Point(Array(1.0, 1.0)))
    val m2 = MBR(Point(Array(1.0, 0.0)), Point(Array(2.0, 1.0)))
    val m3 = MBR(Point(Array(2.0, 0.0)), Point(Array(3.0, 1.0)))

    assert(c.intersects(m1))
    assert(c.intersects(m2))
    assert(!c.intersects(m3))

    assert(Math.abs(c.minDist(m1)) < 1e-8)
    assert(Math.abs(c.minDist(m2)) < 1e-8)
    assert(Math.abs(c.minDist(m3) - 1.0) < 1e-8)
  }
  test("Circle: intersects and minDist with Polygon"){
    val ply1 = Polygon.apply(Array(Point(Array(-1.0, -1.0)), Point(Array(1.0, -1.0)),
      Point(Array(0.0, 1.0)), Point(Array(-1.0, -1.0))))
    val ply2 = Polygon.apply(Array(Point(Array(1.0, 0.0)), Point(Array(2.0, 0.0)),
      Point(Array(2.0, 1.0)), Point(Array(1.0, 0.0))))
    val ply3 = Polygon.apply(Array(Point(Array(2.0, 0.0)), Point(Array(3.0, 0.0)),
      Point(Array(3.0, 1.0)), Point(Array(2.0, 0.0))))

    assert(c.intersects(ply1))
    assert(c.intersects(ply2))
    assert(!c.intersects(ply3))

    assert(Math.abs(c.minDist(ply1)) < 1e-8)
    assert(Math.abs(c.minDist(ply2)) < 1e-8)
    assert(Math.abs(c.minDist(ply3) - 1.0) < 1e-8)
  }

  test("Circle: Construct MBR"){
    assert((c.getMBR == MBR(Point(Array(-1.0, -1.0)), Point(Array(1.0, 1.0)))))
  }
}
