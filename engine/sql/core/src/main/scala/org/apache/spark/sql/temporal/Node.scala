/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.temporal

import org.bdgenomics.adam.models.Interval

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class Node[K <: Interval, T: ClassTag](int: K) extends Serializable {
  val interval: K = int
  var leftChild: Node[K, T] = null
  var rightChild: Node[K, T] = null
  var subtreeMax = int.end


  // DATA SHOULD BE STORED MORE EFFICIENTLY
  var data: ListBuffer[T] = new ListBuffer()

  def this(itvl: K, data: T) = {
    this(itvl)
    put(data)
  }

  def getSize(): Long = {
    data.length
  }

  override def clone: Node[K, T] = {
    val n: Node[K, T] = new Node(interval)
    n.data = data
    n
  }

  def clearChildren() = {
    leftChild = null
    rightChild = null
  }

  def multiput(rs: Iterator[T]) = {
    val newData = rs.toList
    data ++= newData
  }

  def multiput(rs: List[T]) = {
    data ++= rs
  }

  def put(newData: T) = {
    data += newData
  }

  def get(): Iterator[T] = data.toIterator

  def greaterThan(other: K): Boolean = {
      interval.start > other.start
  }

  def equals(other: K): Boolean = {
      (interval.start == other.start && interval.end == other.end)
  }

  def lessThan(other: K): Boolean = {
      interval.start < other.start
  }

  def overlaps(other: K): Boolean = {
    interval.start < other.end && interval.end > other.start
  }

}
