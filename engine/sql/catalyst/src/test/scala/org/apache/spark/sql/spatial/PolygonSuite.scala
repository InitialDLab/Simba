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
  * Created by Zhihao Bai on 16-7-3.
  */
class PolygonSuite extends FunSuite{
  test("Polygon: intersects and minDist with other Polygon"){
    val ply1 = Polygon.apply(Array(Point(Array(-1.0, -1.0)), Point(Array(1.0, -1.0)),
      Point(Array(0.0, 1.0)), Point(Array(-1.0, -1.0))))
    val ply2 = Polygon.apply(Array(Point(Array(0.0, 0.0)), Point(Array(2.0, 0.0)),
      Point(Array(1.0, 2.0)), Point(Array(0.0, 0.0))))
    val ply3 = Polygon.apply(Array(Point(Array(-1.0, 1.0)), Point(Array(1.0, 1.0)),
      Point(Array(0.0, 3.0)), Point(Array(-1.0, 1.0))))
    val ply4 = Polygon.apply(Array(Point(Array(1.0, 0.0)), Point(Array(2.0, 0.0)),
      Point(Array(2.0, 1.0)), Point(Array(1.0, 0.0))))

    assert(ply1.intersects(ply2))
    assert(ply1.intersects(ply3))
    assert(!ply1.intersects(ply4))

    assert(Math.abs(ply1.minDist(ply2)) < 1e-8)
    assert(Math.abs(ply1.minDist(ply3)) < 1e-8)
    assert(Math.abs(ply1.minDist(ply4) - 1.0 / Math.sqrt(5.0)) < 1e-8)
  }

  val ply = Polygon.apply(Array(Point(Array(0.0, 0.0)), Point(Array(2.0, 0.0)),
    Point(Array(1.0, 2.0)), Point(Array(0.0, 0.0))))

  test("Polygon: intersects and minDist with Point"){
    val p1 = Point(Array(1.0, 1.0))
    val p2 = Point(Array(1.5, 1.0))
    val p3 = Point(Array(2.0, 1.0))

    assert(ply.intersects(p1))
//    assert(ply.intersects(p2))
    assert(!ply.intersects(p3))

    assert(Math.abs(ply.minDist(p1)) < 1e-8)
    assert(Math.abs(ply.minDist(p2)) < 1e-8)
    assert(Math.abs(ply.minDist(p3) - 1.0 / Math.sqrt(5.0)) < 1e-8)
  }
  test("Polygon: intersects and minDist with LineSegment"){
    val s1 = LineSegment(Point(Array(1.0, 1.0)), Point(Array(2.0, 1.0)))
    val s2 = LineSegment(Point(Array(0.0, 2.0)), Point(Array(2.0, 2.0)))
    val s3 = LineSegment(Point(Array(3.0, 0.0)), Point(Array(3.0, 2.0)))

    assert(ply.intersects(s1))
    assert(ply.intersects(s2))
    assert(!ply.intersects(s3))

    assert(Math.abs(ply.minDist(s1)) < 1e-8)
    assert(Math.abs(ply.minDist(s2)) < 1e-8)
    assert(Math.abs(ply.minDist(s3) - 1.0) < 1e-8)
  }
  test("Polygon: intersects and minDist with Circle"){
    val c1 = Circle(Point(Array(1.0, 1.0)), 1.0)
    val c2 = Circle(Point(Array(1.0, -1.0)), 1.0)
    val c3 = Circle(Point(Array(4.0, 0.0)), 1.0)

    assert(ply.intersects(c1))
    assert(ply.intersects(c2))
    assert(!ply.intersects(c3))

    assert(Math.abs(ply.minDist(c1)) < 1e-8)
    assert(Math.abs(ply.minDist(c2)) < 1e-8)
    assert(Math.abs(ply.minDist(c3) - 1.0) < 1e-8)
  }

  test("Polygon: intersects and minDist with MBR"){
    val m1 = MBR(Point(Array(1.0, 1.0)), Point(Array(2.0, 2.0)))
    val m2 = MBR(Point(Array(0.0, 2.0)), Point(Array(2.0, 4.0)))
    val m3 = MBR(Point(Array(3.0, 0.0)), Point(Array(4.0, 1.0)))

    assert(ply.intersects(m1))
    assert(ply.intersects(m2))
    assert(!ply.intersects(m3))

    assert(Math.abs(ply.minDist(m1)) < 1e-8)
    assert(Math.abs(ply.minDist(m2)) < 1e-8)
    assert(Math.abs(ply.minDist(m3) - 1.0) < 1e-8)
  }

  test("Polygon: construct MBR"){
    assert((ply.getMBR == MBR(Point(Array(0.0, 0.0)), Point(Array(2.0, 2.0)))))
  }
}
