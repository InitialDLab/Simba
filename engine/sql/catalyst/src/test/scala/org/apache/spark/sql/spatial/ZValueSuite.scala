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
class ZValueSuite extends FunSuite{
  test("Test for ZValue"){
    assert(ZValue.paddingBinaryBits(7, 8).equals("00000111"))

    var point = Array[Int](2, 0, 1, 6)

    var x = ZValue.apply(point)
    assert(x.toBinaryString.equals("110010010"))

    var point1 = ZValue.unapply(x, 4).get
    assert(point.length == point1.length)
    for(i <- point.indices)
      assert(point(i) == point1(i))
  }
}
