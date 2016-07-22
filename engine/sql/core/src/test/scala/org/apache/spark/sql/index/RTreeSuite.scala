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
import org.apache.spark.sql.spatial.{Circle, MBR, Point}

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
    val mbr = MBR(Point(Array(0.0, 0.0)), Point(Array(9.0, 9.0)))
    val range = rtree.range(mbr)

    for(x <-range){
      println("" + x._1.toString + ' ' + x._2)
    }
  }

  test("RTree: range, complex"){

  }

  test("RTree: circleRange"){
  }

  test("RTree: circleRangeConj"){

  }

  test("RTree: kNN"){

  }
}
