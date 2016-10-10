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

import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.FunSuite

/**
  * Created by Zhihao Bai on 16-7-19.
  */
class TreapSuite extends FunSuite{
  val data = new Array[(Int, InternalRow)](101)
  val divisor = 11
  for(i <- 0 to 100) {
    data(i) = (i % divisor, null)
  }

  val treap = Treap.apply[Int](data)

  test("Treap: rank"){
    var rank = 0
    var count = 0
    for(i <- 0 to 10) {
      count = 101 / divisor
      if(i == 0 || i == 1) count = count + 1
      rank += count
      assert(treap.rank(i) == rank)
    }
  }

  test("Treap: getCount"){
    var count = 0
    for(i <- 0 to 10) {
      count = 101 / divisor
      if(i == 0 || i == 1) count = count + 1
      assert(treap.getCount(i) == count)
    }
  }

  test("Treap: find"){
    var count = 0
    for(i <- 0 to 10) {
      count = 101 / divisor
      if(i == 0 || i == 1) count = count + 1
      var ints = treap.find(i)
      assert(ints.length == count)
      for(j <- ints) {
        assert(j % divisor == i)
      }
    }
  }

  test("Treap: range, simple"){
    var ints = treap.range(3, 5)
    for(j <- ints){
      assert(j % divisor >= 3 && j % divisor <= 5)
    }
  }
  test("Treap: range, complex"){
    var ints = treap.range(3, 5, 5, 1.1, true).get
    for(j <- ints){
      assert(j % divisor >= 3 && j % divisor <= 5)
    }

    var ints_none = treap.range(0, 10, 1, 0.0, true)
    assert(ints_none == None)
  }
}
