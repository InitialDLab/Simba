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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.spatial.{MBR, Point}

/**
  * Created by Zhihao Bai on 16-8-3.
  */
class QuadTreeSuite extends SparkFunSuite{
  var entries = new Array[(Point, Int)](221)
  var cnt = 0
  for (i <- -10 to 10){
    for (j <- -10 to 10){
      if (Math.abs(i) + Math.abs(j) <= 10) {
        entries(cnt) = (new Point(Array(i, j)), i + j)
        cnt = cnt + 1
      }
    }
  }
  val quadtree = QuadTree.apply(entries)

  def legal(x: (Double, Double, Int)): Boolean = {
    val a = x._1.toInt
    val b = x._2.toInt
    if(a < 0 || a > 10) false
    else if (b < 0 || b > 10) false
    else if (Math.abs(a) + Math.abs(b) > 10) false
    else if (a + b != x._3) false
    else true
  }

  def legal(x: (Point, Int)): Boolean = legal(x._1.coord(0), x._1.coord(1), x._2)

  test("QuadTree: range(doubles)"){
    val range = quadtree.range(0.0, 0.0, 9.0, 9.0, searchMBR = false)

    range.foreach(x => assert(legal(x)))

    var count = 0
    for (i <- -10 to 10){
      for (j <- -10 to 10){
        if (Math.abs(i) + Math.abs(j) <= 10) {
          if (i >= 0 && j >= 0 && i <= 9 && j <= 9){
            count = count + 1
          }
        }
      }
    }
    assert(range.length == count)
  }

  test("QuadTree: range(MBR)"){
    val A = Point(Array(0.0, 0.0))
    val B = Point(Array(9.0, 9.0))
    val mbr = MBR(A, B)
    val range = quadtree.range(mbr)

    range.foreach(x => assert(legal(x)))

    var count = 0;
    for (i <- -10 to 10){
      for (j <- -10 to 10){
        if (Math.abs(i) + Math.abs(j) <= 10) {
          if (i >= 0 && j >= 0 && i <= 9 && j <= 9) {
            count = count + 1
          }
        }
      }
    }
    assert(range.length == count)
  }
}
