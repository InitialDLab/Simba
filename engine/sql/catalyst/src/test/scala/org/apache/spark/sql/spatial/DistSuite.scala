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
  * Created by Zhihao Bai on 16-7-4.
  */
class DistSuite extends FunSuite{
  test("Dist: furthest distance from a point to an MBR"){
    val m = MBR(Point(Array(0.0, 0.0)), Point(Array(2.0, 2.0)))
    val p1 = Point(Array(1.0, 1.0))
    val p2 = Point(Array(0.0, 0.0))
    val p3 = Point(Array(1.0, 3.0))

    assert(Math.abs(Dist.furthest(p1, m) - Math.sqrt(2.0)) < 1e-8)
    assert(Math.abs(Dist.furthest(p2, m) - Math.sqrt(8.0)) < 1e-8)
    assert(Math.abs(Dist.furthest(p3, m) - Math.sqrt(10.0)) < 1e-8)
  }
}
