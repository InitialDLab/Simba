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

package org.apache.spark.sql.simba.spatial

import org.scalatest.FunSuite

/**
  * Created by Zhihao Bai on 16-7-3.
  */
class MBRSuite extends FunSuite{
  test("MBR: intersects and minDist with other MBR"){
    val m1 = MBR(Point(Array(0.0, 0.0)), Point(Array(2.0, 2.0)))
    val m2 = MBR(Point(Array(1.0, 1.0)), Point(Array(3.0, 3.0)))
    val m3 = MBR(Point(Array(2.0, 0.0)), Point(Array(3.0, 1.0)))
    val m4 = MBR(Point(Array(1.0, 3.0)), Point(Array(2.0, 4.0)))

    assert(m1.intersects(m2))
    assert(m1.intersects(m3))
    assert(!m1.intersects(m4))

    assert(Math.abs(m1.minDist(m2)) < 1e-8)
    assert(Math.abs(m1.minDist(m3)) < 1e-8)
    assert(Math.abs(m1.minDist(m4) - 1.0) < 1e-8)
  }

  val m = MBR(Point(Array(0.0, 0.0)), Point(Array(2.0, 2.0)))
  test("MBR: intersects and minDist with Point"){
    val p1 = Point(Array(1.0,1.0))
    val p2 = Point(Array(2.0,  1.0))
    val p3 = Point(Array(3.0, 0.0))

    assert(m.intersects(p1))
    assert(m.intersects(p2))
    assert(!m.intersects(p3))

    assert(Math.abs(m.minDist(p1)) < 1e-8)
    assert(Math.abs(m.minDist(p2)) < 1e-8)
    assert(Math.abs(m.minDist(p3) - 1.0) < 1e-8)
  }
  test("MBR: intersects and minDist with LineSegment"){
    val l1 = LineSegment(Point(Array(1.0, 1.0)), Point(Array(3.0, 2.0)))
    val l2 = LineSegment(Point(Array(1.0, 3.0)), Point(Array(3.0, 1.0)))
    val l3 = LineSegment(Point(Array(3.0, 3.0)), Point(Array(4.0, 2.0)))

    assert(m.intersects(l1))
    assert(m.intersects(l2))
    assert(!m.intersects(l3))

    assert(Math.abs(m.minDist(l1)) < 1e-8)
    assert(Math.abs(m.minDist(l2)) < 1e-8)
    assert(Math.abs(m.minDist(l3) - Math.sqrt(2.0)) < 1e-8)
  }
  test("MBR: intersects and minDist with Circle"){
    val c1 = Circle(Point(Array(2.0, 1.0)), 1.0)
    val c2 = Circle(Point(Array(3.0, 3.0)), Math.sqrt(2.0))
    val c3 = Circle(Point(Array(4.0, 1.0)), 1.0)

    assert(m.intersects(c1))
    assert(m.intersects(c2))
    assert(!m.intersects(c3))

    assert(Math.abs(m.minDist(c1)) < 1e-8)
    assert(Math.abs(m.minDist(c2)) < 1e-8)
    assert(Math.abs(m.minDist(c3) - 1.0) < 1e-8)
  }
  test("MBR: intersects and minDist with Polygon"){
    val ply1 = Polygon.apply(Array(Point(Array(-1.0, -1.0)), Point(Array(1.0, -1.0)),
      Point(Array(0.0, 1.0)), Point(Array(-1.0, -1.0))))
    val ply2 = Polygon.apply(Array(Point(Array(-2.0, -1.0)), Point(Array(0.0, -1.0)),
      Point(Array(0.0, 1.0)), Point(Array(-2.0, -1.0))))
    val ply3 = Polygon.apply(Array(Point(Array(2.0, -1.0)), Point(Array(3.0, -1.0)),
      Point(Array(3.0, 0.0)), Point(Array(2.0, -1.0))))

    assert(m.intersects(ply1))
    assert(m.intersects(ply2))
    assert(!m.intersects(ply3))

    assert(Math.abs(m.minDist(ply1)) < 1e-8)
    assert(Math.abs(m.minDist(ply2)) < 1e-8)
    assert(Math.abs(m.minDist(ply3) - Math.sqrt(2.0) / 2.0) < 1e-8)
  }

  test("MBR: area"){
    assert(Math.abs(m.area - 4.0) < 1e-8)
  }

  test("MBR: ratio"){
    val m1 = MBR(Point(Array(1.0, 1.0)), Point(Array(3.0,  3.0)))
    assert(Math.abs(m.calcRatio(m1) - 0.25) < 1e-8)
  }

  test("MBR: getMBR"){
    assert(m.getMBR == MBR(Point(Array(0.0, 0.0)), Point(Array(2.0, 2.0))))
  }
}
