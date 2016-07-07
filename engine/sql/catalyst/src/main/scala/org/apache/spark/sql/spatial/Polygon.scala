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

import com.vividsolutions.jts.geom.{Polygon => JTSPolygon, LineSegment => JTSLineSegment}
import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import com.vividsolutions.jts.io.{WKBWriter, WKBReader, WKTWriter}

/**
  * Created by Dong Xie on 3/16/2016.
  * Light wraper of JTS Polygon
  * Note: Only support up to 2 dimension
  */
case class Polygon(content: JTSPolygon) extends Shape {
  def this() = {
    this(null)
  }

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

  def intersects(seg: LineSegment): Boolean = {
    val start = new Coordinate(seg.start.coord(0), seg.start.coord(1))
    val end = new Coordinate(seg.end.coord(0), seg.end.coord(1))
    content.intersects(gf.createLineString(Array(start, end)))
  }

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

  def minDist(cir: Circle): Double = {
    val res = minDist(cir.center) - cir.radius
    if (res <= 0) 0
    else res
  }


  def minDist(poly: Polygon): Double = content.distance(poly.content)

  def minDist(seg: LineSegment): Double = {
    val start = new Coordinate(seg.start.coord(0), seg.start.coord(1))
    val end = new Coordinate(seg.end.coord(0), seg.end.coord(1))
    content.distance(gf.createLineString(Array(start, end)))
  }

  override def toString: String = new WKTWriter().write(content)
  def toWKB: Array[Byte] = new WKBWriter().write(content)

  def getMBR: MBR = {
    val envelope = content.getEnvelopeInternal
    new MBR(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
  }
}

object Polygon {
  def apply(points: Array[Point]): Polygon = {
    require(points.length > 2 && points(0).coord.length == 2)
    val gf = new GeometryFactory()
    Polygon(gf.createPolygon(points.map(x => new Coordinate(x.coord(0), x.coord(1)))))
  }
  def fromJTSPolygon(polygon: JTSPolygon): Polygon = new Polygon(polygon)
  def fromWKB(bytes: Array[Byte]): Polygon =
    new Polygon(new WKBReader().read(bytes).asInstanceOf[JTSPolygon])
}
