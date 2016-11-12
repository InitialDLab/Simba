/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package edu.utah.cs.simba.util

import java.io.Serializable
import java.util.PriorityQueue

import scala.collection.JavaConverters._
import scala.collection.generic.Growable

/**
  * Bounded priority queue. This class wraps the original PriorityQueue
  * class and modifies it such that only the top K elements are retained.
  * The top K elements are defined by an implicit Ordering[T].
  */
private[simba] class BoundedPriorityQueue[T](maxSize: Int)(implicit ord: Ordering[T])
  extends Iterable[T] with Growable[T] with Serializable {

  private val underlying = new PriorityQueue[T](maxSize, ord)

  override def iterator: Iterator[T] = underlying.iterator.asScala

  override def size: Int = underlying.size

  override def ++=(xs: TraversableOnce[T]): this.type = {
    xs.foreach { this += _ }
    this
  }

  override def +=(elem: T): this.type = {
    if (size < maxSize) {
      underlying.offer(elem)
    } else {
      maybeReplaceLowest(elem)
    }
    this
  }

  override def +=(elem1: T, elem2: T, elems: T*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def clear() { underlying.clear() }

  private def maybeReplaceLowest(a: T): Boolean = {
    val head = underlying.peek()
    if (head != null && ord.gt(a, head)) {
      underlying.poll()
      underlying.offer(a)
    } else {
      false
    }
  }
}