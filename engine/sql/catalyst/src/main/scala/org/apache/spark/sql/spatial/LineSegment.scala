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

/**
  * Created by dongx on 5/31/16.
  * This is a simple implementation for Line Segment.
  * Note: Currently, we only support 2D line segments.
  */
case class LineSegment(start: Point, end: Point) extends Shape {
  require(start.coord.length == 2 && end.coord.length == 2)

  override def intersects(other: Shape): Boolean = {
    other match {
      case p: Point => contains(p)
      case mbr: MBR => intersects(mbr)
      case cir: Circle => intersects(cir)
      case poly: Polygon => poly.intersects(this)
      case seg: LineSegment => intersects(seg)
    }
  }

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
      case mbr: MBR => minDist(mbr)
      case cir: Circle => minDist(cir)
      case poly: Polygon => poly.minDist(this)
      case seg: LineSegment => minDist(seg)
    }
  }

  private def orientation(p: Point, q: Point, r: Point): Int = {
    val cross = (q.coord(1) - p.coord(1)) * (r.coord(0) - q.coord(0)) -
      (q.coord(0) - p.coord(0)) * (r.coord(1) - q.coord(1))
    if (cross == 0) 0
    else if (cross > 0) 1
    else -1
  }

  private def withinBox(check: Point, start: Point, end: Point): Boolean = {
    if (check.coord(0) >= Math.min(start.coord(0), end.coord(0)) &&
      check.coord(0) <= Math.max(start.coord(0), end.coord(0)) &&
      check.coord(1) >= Math.min(start.coord(1), end.coord(1)) &&
      check.coord(1) <= Math.max(start.coord(1), end.coord(1))) {
      true
    } else false
  }

  def intersects(l: LineSegment): Boolean = intersects(l.start, l.end)

  private def intersects(p: Point, q: Point): Boolean = {
    val o1 = orientation(start, end, p)
    val o2 = orientation(start, end, q)
    val o3 = orientation(p, q, start)
    val o4 = orientation(p, q, end)
    if (o1 != o2 && o3 != o4) true
    else if (o1 == 0 && withinBox(p, start, end)) true
    else if (o2 == 0 && withinBox(q, start, end)) true
    else if (o3 == 0 && withinBox(start, p, q)) true
    else if (o4 == 0 && withinBox(end, p, q)) true
    else false
  }

  def contains(l: Point): Boolean = orientation(start, l, end) == 0 && withinBox(l, start, end)

  def intersects(cir: Circle): Boolean = {
    minDist(cir.center) <= cir.radius
  }

  def intersects(mbr: MBR): Boolean = {
    assert(mbr.low.coord.length == 2)
    if (intersects(mbr.low, Point(Array(mbr.high.coord(0), mbr.low.coord(1))))) true
    else if (intersects(mbr.low, Point(Array(mbr.low.coord(0), mbr.high.coord(1))))) true
    else if (intersects(mbr.high, Point(Array(mbr.high.coord(0), mbr.low.coord(1))))) true
    else if (intersects(mbr.high, Point(Array(mbr.low.coord(0), mbr.high.coord(1))))) true
    else false
  }

  def minDist(p: Point): Double = {
    require(p.coord.length == 2)
    val len = start.minDist(end)
    if (len == 0) return p.minDist(start)
    var t = ((p.coord(0) - start.coord(0)) * (end.coord(0) - start.coord(0))
      + (p.coord(1) - start.coord(1)) * (end.coord(1) - start.coord(1))) / (len * len)
    t = Math.max(0, Math.min(1, t))
    val proj_x = start.coord(0) + t * (end.coord(0) - start.coord(0))
    val proj_y = start.coord(1) + t * (end.coord(1) - start.coord(1))
    p.minDist(Point(Array(proj_x, proj_y)))
  }

  def minDist(cir: Circle): Double = {
    val centeral_dis = minDist(cir.center)
    if (centeral_dis <= cir.radius) 0.0
    else centeral_dis - cir.radius
  }

  def minDist(l: LineSegment): Double = {
    if (intersects(l)) 0.0
    else {
      Math.min(Math.min(minDist(l.start), minDist(l.end)),
        Math.min(l.minDist(start), l.minDist(end)))
    }
  }

  def minDist(mbr: MBR): Double = {
    val s1 = LineSegment(mbr.low, Point(Array(mbr.low.coord(0), mbr.high.coord(1))))
    val s2 = LineSegment(mbr.low, Point(Array(mbr.high.coord(0), mbr.low.coord(1))))
    val s3 = LineSegment(mbr.high, Point(Array(mbr.low.coord(0), mbr.high.coord(1))))
    val s4 = LineSegment(mbr.high, Point(Array(mbr.high.coord(0), mbr.low.coord(1))))
    Math.min(Math.min(minDist(s1), minDist(s2)), Math.min(minDist(s3), minDist(s4)))
  }

  override def getMBR: MBR = {
    val (low_x, high_x) = if (start.coord(0) < end.coord(0)) {
      (start.coord(0), end.coord(0))
    } else {
      (end.coord(0), start.coord(0))
    }

    val (low_y, high_y) = if (start.coord(1) < end.coord(1)) {
      (start.coord(1), end.coord(1))
    } else {
      (end.coord(1), start.coord(1))
    }

    MBR(Point(Array(low_x, low_y)), Point(Array(high_x, high_y)))
  }

  override def toString: String = "SEG(" + start.toString + "->" + end.toString + ")"
}
