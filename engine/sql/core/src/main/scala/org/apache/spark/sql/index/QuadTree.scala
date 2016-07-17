/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.spark.sql.index

import org.apache.spark.sql.spatial.{MBR, Point, Shape}
import scala.collection._

/**
  * Created by gefei on 16-6-10.
  */

import collection.mutable

class Quadtree[A <% (Int) => Double](xmin: Double, xmax: Double,
                                   ymin: Double, ymax: Double) {
  private val MaxObjs = 3

  private class Node(cx: Double, cy: Double, sx: Double, sy: Double,
                     val objects: mutable.Buffer[A], var children: Array[Node]) {
    def whichChild(obj: A): Int = {
      (if (obj(0) > cx) 1 else 0) + (if (obj(1) > cy) 2 else 0)
    }
    def makeChildren() {
      children = Array(
        new Node(cx - sx/4, cy - sy/4, sx/2, sy/2, mutable.Buffer(), null),
        new Node(cx + sx/4, cy - sy/4, sx/2, sy/2, mutable.Buffer(), null),
        new Node(cx - sx/4, cy + sy/4, sx/2, sy/2, mutable.Buffer(), null),
        new Node(cx + sx/4, cy + sy/4, sx/2, sy/2, mutable.Buffer(), null)
      )
    }
    def overlap(obj: A, radius: Double): Boolean = {
      obj(0) - radius < cx + sx/2 && obj(0) + radius > cx - sx/2 &&
        obj(1) - radius < cy + sy/2 && obj(1) + radius > cy - sy/2
    }
    def overlap(low: A, high: A): Boolean = {
      val left_upper = new A(low(0), high(1))
      val right_bottom = new A(high(1), low(0))
      this.overlap(low, 0) || this.overlap(high, 0) ||
        this.overlap(left_upper, 0) || this.overlap(right_bottom, 0)
    }
  }

  private val root = new Node((xmax + xmin)/2, (ymax + ymin)/2, xmax-xmin, ymax-ymin,
    mutable.Buffer[A](), null)

  def add(obj: A) {
    addRecur(obj, root)
  }

  private def addRecur(obj: A, n: Node) {
    if (n.children == null) {
      if (n.objects.length < MaxObjs) n.objects += obj
      else {
        n.makeChildren()
        for(o <- n.objects) {
          addRecur(o, n.children(n.whichChild(o)))
        }
        n.objects.clear
        addRecur(obj, n.children(n.whichChild(obj)))
      }
    } else {
      addRecur(obj, n.children(n.whichChild(obj)))
    }
  }

  def searchNeighbors(obj: A, radius: Double): mutable.Buffer[A] = {
    val ret = mutable.Buffer[A]()
    searchRecur(obj, radius, root, ret)
    ret
  }

  private def searchRecur(obj: A, radius: Double, n: Node, ret: mutable.Buffer[A]) {
    if (n.children == null) {
      ret ++= n.objects.filter(o => distance(o, obj) < radius)
    } else {
      for (child <- n.children; if (!child.overlap(obj, radius)))
        searchRecur(obj, radius, child, ret)
    }
  }

  def rectSearch(low: A, high: A): mutable.Buffer[A] = {
    val ret = mutable.Buffer[A]()
    rectSearchRecur(low, high, root, ret)
    ret
  }

  private def rectSearchRecur(low: A, high: A, n: Node, ret: mutable.Buffer[A]): Unit = {
    if (n.children == null){
      ret ++= n.objects.filter(o => inRect(o, low, high))
    } else {
      for (child <- n.children)
        if (child.overlap(low, high)) rectSearchRecur(low, high, child, ret)
    }
  }

  private def distance(a: A, b: A): Double = {
    val dx = a(0) - b(0)
    val dy = a(1) - b(1)
    math.sqrt(dx * dx + dy * dy)
  }

  private def inRect(obj: A, low: A, high: A): Boolean = {
    obj(0) >= low(0) && obj(0) <= high(0) && obj(1) >= low(1) && obj(1) <= high(1)
  }
}