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

package org.apache.spark.sql.spatial

import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.{Envelope, GeometryFactory, Coordinate}
import com.vividsolutions.jts.io.WKTWriter

/**
  * Created by Dong Xie on 3/16/2016.
  * Light wraper of JTS Polygon
  * Note: Only support up to 2 dimension
  */
case class Polygon(content: geom.Polygon) extends Shape {
  val gf = new GeometryFactory()

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
      case mbr: MBR => minDist(mbr)
      case cir: Circle => minDist(cir)
      case poly: Polygon => minDist(poly)
    }
  }

  override def intersects(other: Shape): Boolean = {
    other match {
      case p: Point => contains(p)
      case mbr: MBR => intersects(mbr)
      case cir: Circle => intersects(cir)
      case poly: Polygon => intersects(poly)
    }
  }

  def contains(p: Point): Boolean = {
    require(p.coord.length == 2)
    content.contains(gf.createPoint(new Coordinate(p.coord(0), p.coord(1))))
  }

  def intersects(mbr: MBR): Boolean = {
    require(mbr.low.coord.length == 2)
    val low = new Coordinate(mbr.low.coord(0), mbr.low.coord(1))
    val high = new Coordinate(mbr.high.coord(0), mbr.high.coord(1))
    content.intersects(gf.toGeometry(new Envelope(low, high)))
  }

  def intersects(cir: Circle): Boolean = minDist(cir.center) <= cir.radius

  def intersects(poly: Polygon): Boolean = content.intersects(poly.content)

  def minDist(p: Point): Double = {
    require(p.coord.length == 2)
    content.distance(gf.createPoint(new Coordinate(p.coord(0), p.coord(1))))
  }

  def minDist(mbr: MBR): Double = {
    require(mbr.low.coord.length == 2)
    val low = new Coordinate(mbr.low.coord(0), mbr.low.coord(1))
    val high = new Coordinate(mbr.high.coord(0), mbr.high.coord(1))
    content.distance(gf.toGeometry(new Envelope(low, high)))
  }

  def minDist(cir: Circle): Double = minDist(cir.center) - cir.radius

  def minDist(poly: Polygon): Double = content.distance(poly.content)

  override def toString: String = new WKTWriter().write(content)
}

object Polygon {
  def apply(points: Array[Point]): Polygon = {
    require(points.length > 2 && points(0).coord.length == 2)
    val gf = new GeometryFactory()
    Polygon(gf.createPolygon(points.map(x => new Coordinate(x.coord(0), x.coord(1)))))
  }
}
