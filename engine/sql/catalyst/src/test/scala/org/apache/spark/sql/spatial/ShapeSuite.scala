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
import com.vividsolutions.jts.geom.{GeometryFactory, Polygon => JTSPolygon}
import com.vividsolutions.jts.io.WKTReader

/**
  * Created by Zhihao Bai on 16-7-5.
  */
class ShapeSuite extends FunSuite{
  def intersects(s1: Shape, s2: Shape): Boolean = {
    s1.intersects(s2)
  }

  def minDist(s1: Shape,  s2: Shape): Double = {
    s1.minDist(s2)
  }

  test("Shape: Abstract Functions"){
    val p = Point(Array(-1.0, 0.0))
    val s = LineSegment(Point(Array(-1.0, 0.0)), Point(Array(1.0, 1.0)))
    val m = MBR(Point(Array(0.0, 0.0)), Point(Array(2.0, 2.0)))
    val c = Circle(Point(Array(0.0, -1.0)), 1.0)

    assert(!intersects(p, c))
    assert(intersects(s, m))

    assert(Math.abs(minDist(p, c) - (Math.sqrt(2.0) - 1.0)) < 1e-8)
    assert(Math.abs(minDist(s, m)) < 1e-8)
  }

  test("Shape: apply Geometry"){
    val gf = new GeometryFactory()
    val reader = new WKTReader( gf );

    val point = reader.read("POINT (0.0 0.0)")
    assert(Shape.apply(point) == null)

    val ply = Shape.apply(reader.read("POLYGON((2.0 1.0, 3.0 0.0, 4.0 1.0, 3.0 2.0, 2.0 1.0))"))
    assert(ply != null)
    val p = Point(Array(2.0, 0.0))
    assert(Math.abs(ply.minDist(p) - Math.sqrt(2.0) / 2.0) < 1e-8)
  }
}
