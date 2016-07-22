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

import org.scalatest.FunSuite
import org.apache.spark.sql.spatial.{Circle, MBR, Point, Shape}

/**
  * Created by Zhihao Bai on 16-7-19.
  */
class RTreeSuite extends FunSuite{
  var entries = new Array[(Point, Int)](221)
  var cnt = 0
  for (i <- -10 to 10){
    for(j <- -10 to 10){
      if(Math.abs(i) + Math.abs(j) <= 10) {
        entries(cnt) = (new Point(Array(i, j)), i + j)
        cnt = cnt + 1
      }
    }
  }
  val rtree = RTree.apply(entries, 5)

  test("RTree: range, simple"){
    val A = Point(Array(0.0, 0.0))
    val B = Point(Array(9.0, 9.0))
    val mbr = MBR(A, B)
    val range = rtree.range(mbr)

    for(x <-range){
      assert(mbr.intersects(x._1))
    }

    var count = 0;
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          if(i >= 0 && j >= 0 && i <= 9 && j <= 9)
            count = count + 1
        }
      }
    }
    assert(range.length == count)
  }

  test("RTree: range, complex"){
    val A = Point(Array(0.0, 0.0))
    val B = Point(Array(9.0, 9.0))
    val mbr = MBR(A, B)

    val ans = rtree.range(mbr, 10, 1.0)
    assert(ans != None)
    val range = ans.get
    for(x <-range){
      assert(mbr.intersects(x._1))
    }
    var count = 0;
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          if(i >= 0 && j >= 0 && i <= 9 && j <= 9)
            count = count + 1
        }
      }
    }
    assert(range.length == count)

    val no_ans = rtree.range(mbr, 1, 0.0)
    assert(no_ans == None)
  }

  test("RTree: circleRange"){
    val center = Point(Array(6.0, 6.0))
    val  radius = 6.0
    val  circle = Circle(center, radius)
    val range = rtree.circleRange(center, radius)

    for(x <-range){
      assert(circle.intersects(x._1))
    }

    var count = 0;
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          if((i - 6) * (i - 6) + (j - 6) * (j - 6) <= 36)
            count = count + 1
        }
      }
    }
    assert(range.length == count)
  }

  test("RTree: circleRangeConj"){
    val A = Point(Array(6.0, 6.0))
    val B = Point(Array(-3.0, 0.0))
    val C = Point(Array(0.0, -5.0))
    val circles = Array[(Point, Double)]((A, 10.0),  (B, 3.0), (C, 7.0));

    val range = rtree.circleRangeConj(circles)

    def touch(p: Shape): Boolean = {
      for((q, d) <- circles){
        if(q.minDist(p) > d) false
      }
      true
    }

    for(x <-range){
      assert(touch(x._1))
    }

    var count = 0;
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          if((i - 6) * (i - 6) + (j - 6) * (j - 6) <= 100
            && (i + 3) * (i + 3) + (j - 0) * (j - 0) <= 9
            && (i - 0) * (i - 0) + (j + 5) * (j + 5) <= 49)
            count = count + 1
        }
      }
    }
    assert(range.length == count)
  }

  test("RTree: kNN"){
    val center = Point(Array(10.0, 9.0))
    val k = 4
    val range = rtree.kNN(center, k)

    def minDist(p: Point): Double = range.map(x => x._1.minDist(p)).min

    assert(range.length == 4)
    assert(minDist(Point(Array(7.0, 3.0))) < 1e-8)
    assert(minDist(Point(Array(6.0, 4.0))) < 1e-8)
    assert(minDist(Point(Array(5.0, 5.0))) < 1e-8)
    assert(minDist(Point(Array(4.0, 6.0))) < 1e-8)
  }
}
