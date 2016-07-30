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
import org.scalatest.PrivateMethodTester._
/**
  * Created by Zhihao Bai on 16-7-2.
  */
class LineSegmentSuite extends FunSuite{
  val s1 = LineSegment(Point(Array(-1.0, 0.0)), Point(Array(1.0, 1.0)))
  val s2 = LineSegment(Point(Array(0.0,  0.0)), Point(Array(0.0, 2.0)))
  val s3 = LineSegment(Point(Array(1.0, 0.0)), Point(Array(1.0, 2.0)))
  val s4 = LineSegment(Point(Array(2.0, 0.0)),  Point(Array(2.0, 2.0)))

  test("LineSegment: intersects with other LineSegment"){
    assert(s1.intersects(s2))
    assert(s1.intersects(s3))
    assert(!s1.intersects(s4))
  }

  test("LineSegment: minDist with other LineSegment"){
    assert(s1.minDist(s2) == 0.0)
    assert(s1.minDist(s3) == 0.0)
    assert(Math.abs(s1.minDist(s4) - 1.0) < 1e-6)
  }

  val s = LineSegment(Point(Array(-1.0, 0.0)), Point(Array(1.0, 1.0)))

  test("LineSegment: intersect and minDist with Point"){
    val p1 = Point(Array(0.0, 0.5))
    val p2 = Point(Array(1.0, 1.0))
    val p3 = Point(Array(1.0, 0.0))

    assert(s.intersects(p1))
    assert(s.intersects(p2))
    assert(!s.intersects(p3))

    assert(Math.abs(s.minDist(p1)) < 1e-8)
    assert(Math.abs(s.minDist(p2)) < 1e-8)
    assert(Math.abs(s.minDist(p3) - 2.0 / Math.sqrt(5.0)) < 1e-8)
  }

  test("LineSegment: intersect and minDist with MBR"){
    val m1 = MBR(Point(Array(-2.0, -1.0)), Point(Array(0.0, 2.0)))
    val m2 = MBR(Point(Array(-2.0, -1.0)), Point(Array(-1.0, 0.0)))
    val m3 = MBR(Point(Array(1.0, -1.0)), Point(Array(2.0, 0.0)))

    assert(s.intersects(m1))
    assert(s.intersects(m2))
    assert(!s.intersects(m3))

    assert(Math.abs(s.minDist(m1)) < 1e-8)
    assert(Math.abs(s.minDist(m2)) < 1e-8)
    assert(Math.abs(s.minDist(m3) - 2.0 / Math.sqrt(5.0)) < 1e-8)
  }

  test("LineSegment: intersect and minDist with Circle"){
    val c1 = Circle(Point(Array(0.0, 0.0)), 1.0)
    val c2 = Circle(Point(Array(0.25, 0.0)), Math.sqrt(5.0) / 4.0)
    val c3 = Circle(Point(Array(2.0, 0.0)), 1.0)

    assert(s.intersects(c1))
    assert(s.intersects(c2))
    assert(!s.intersects(c3))

    assert(Math.abs(s.minDist(c1)) < 1e-8)
    assert(Math.abs(s.minDist(c2)) < 1e-8)
    assert(Math.abs(s.minDist(c3) - (Math.sqrt(2.0) - 1.0)) < 1e-8)
  }

  test("LineSegment: intersect and minDist with Polygon"){
    val ply1 = Polygon.apply(Array(Point(Array(-1.0, -1.0)), Point(Array(1.0, -1.0)),
      Point(Array(0.0, 1.0)), Point(Array(-1.0, -1.0))))
    val ply2 = Polygon.apply(Array(Point(Array(0.0, -1.0)), Point(Array(1.0, -1.0)),
      Point(Array(1.0, 1.0)), Point(Array(0.0, -1.0))))
    val ply3 = Polygon.apply(Array(Point(Array(1.0, 0.0)), Point(Array(2.0, 0.0)),
      Point(Array(2.0, 1.0)), Point(Array(1.0, 0.0))))

    assert(s.intersects(ply1))
    assert(s.intersects(ply2))
    assert(!s.intersects(ply3))

    assert(Math.abs(s.minDist(ply1)) < 1e-8)
    assert(Math.abs(s.minDist(ply2)) < 1e-8)
    assert(Math.abs(s.minDist(ply3) - Math.sqrt(2.0) / 2.0) < 1e-8)
  }

  test("LineSegment: Construct MBR"){
    val  seg = LineSegment(Point(Array(-1.0, 1.0)), Point(Array(1.0, 0.0)))
    assert((seg.getMBR == MBR(Point(Array(-1.0, 0.0)), Point(Array(1.0, 1.0)))))
  }
}
