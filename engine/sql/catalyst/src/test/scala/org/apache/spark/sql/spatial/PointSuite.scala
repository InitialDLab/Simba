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
class PointSuite extends FunSuite{
  val p1 = Point(Array(0.0, 0.0))
  val p2 = Point(Array(1.0, 1.0))
  val p3 = Point(Array(1.0, 1.0))

  test("Point: intersects with other Point"){
    assert(!p1.intersects(p2))
    assert(p2.intersects(p3))
  }
  test("Point: minDist with other point"){
    assert(Math.abs(p1.minDist(p2) - Math.sqrt(2.0)) < 1e-8)
    assert(p2.minDist(p3) == 0.0)
  }
  test("Point: equals another"){
    assert(!(p1 == p2))
    assert(p2 == p3)
  }
  test("Point: less than another"){
    assert(p1 <= p2)
    assert(p2 <= p3)
    assert(!(p2  <= p1))
  }
  test("Shift one point"){
    assert(p1.shift(1.0) == p2)
  }


  val p = Point(Array(0.0, 0.0))

  test("Point: intersects and minDist with MBR"){
    val m1 = MBR(Point(Array(-2.0, 1.0)), Point(Array(-1.0, 2.0)))
    val m2 = MBR(Point(Array(-1.0, -1.0)), Point(Array(0.0, 0.0)))
    val m3 = MBR(Point(Array(-1.0, -1.0)), Point(Array(1.0, 1.0)))

    assert(!p.intersects(m1))
    assert(p.intersects(m2))
    assert(p.intersects(m3))

    assert(Math.abs(p.minDist(m1) - Math.sqrt(2)) < 1e-8)
    assert(Math.abs(p.minDist(m2)) < 1e-8)
    assert(Math.abs(p.minDist(m3)) < 1e-8)
  }

  test("Point: intersects and minDist with Circle"){
    val c1 = Circle(Point(Array(2.0, 0.0)), 1.0)
    val c2 = Circle(Point(Array(1.0, 0.0)), 1.0)
    val c3 = Circle(Point(Array(0.0, 0.0)), 1.0)

    assert(!p.intersects(c1))
    assert(p.intersects(c2))
    assert(p.intersects(c3))

    assert(Math.abs(p.minDist(c1) - 1.0) < 1e-8)
    assert(Math.abs(p.minDist(c2)) < 1e-8)
    assert(Math.abs(p.minDist(c3)) < 1e-8)
  }
  test("Point: intersects and minDist with LineSegment"){
    val s1 = LineSegment(Point(Array(1.0, 1.0)), Point(Array(2.0, 1.0)))
    val s2 = LineSegment(Point(Array(0.0, 0.0)), Point(Array(1.0, 0.0)))
    val s3 = LineSegment(Point(Array(-1.0, 0.0)), Point(Array(1.0, 0.0)))

    assert(!p.intersects(s1))
    assert(p.intersects(s2))
    assert(p.intersects(s3))

    assert(Math.abs(p.minDist(s1) - Math.sqrt(2.0)) < 1e-8)
    assert(Math.abs(p.minDist(s2)) < 1e-8)
    assert(Math.abs(p.minDist(s3)) < 1e-8)
  }

  test("Point: intersects and minDist with Polygon"){
    val ply1 = Polygon.apply(Array(Point(Array(-1.0, -1.0)), Point(Array(1.0, -1.0)),
      Point(Array(0.0, 1.0)), Point(Array(-1.0, -1.0))))
    val ply2 = Polygon.apply(Array(Point(Array(0.0, 0.0)), Point(Array(4.0, 0.0)),
      Point(Array(3.0, 2.0)), Point(Array(0.0, 0.0))))
    val ply3 = Polygon.apply(Array(Point(Array(1.0, -1.0)), Point(Array(2.0, 1.0)),
      Point(Array(1.0, 1.0)), Point(Array(1.0, -1.0))))

    assert(p.intersects(ply1))
//    assert(p.intersects(ply2))
    assert(!p.intersects(ply3))

    assert(Math.abs(p.minDist(ply1)) < 1e-8)
    assert(Math.abs(p.minDist(ply2)) < 1e-8)
    assert(Math.abs(p.minDist(ply3) - 1.0) < 1e-8)
  }

  test("Point: Construct MBR"){
    assert((p.getMBR == MBR(Point(Array(0.0, 0.0)), Point(Array(0.0, 0.0)))))
  }
}
