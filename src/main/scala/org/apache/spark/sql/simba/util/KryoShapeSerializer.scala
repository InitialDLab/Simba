/*
 * Copyright 2017 by Simba Project
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

package org.apache.spark.sql.simba.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.vividsolutions.jts.geom.{CoordinateSequence, GeometryFactory, LinearRing}
import org.apache.spark.sql.simba.spatial._

/**
  * Created by dongx on 11/15/16.
  */
class KryoShapeSerializer extends Serializer[Shape] {
  val gf = new GeometryFactory()
  val csFactory = gf.getCoordinateSequenceFactory

  private def getTypeInt(o: Shape): Short = o match {
    case _: Point => 0
    case _: MBR => 1
    case _: Circle => 2
    case _: Polygon => 3
    case _: LineSegment => 4
    case _ => -1
  }

  private def writeCoordSequence(output: Output, coords: CoordinateSequence) = {
    output.writeInt(coords.size(), true)
    for (i <- 0 until coords.size()) {
      val coord = coords.getCoordinate(i)
      output.writeDouble(coord.getOrdinate(0))
      output.writeDouble(coord.getOrdinate(1))
    }
  }

  private def readCoordSequence(input: Input) = {
    val n = input.readInt(true)
    val coords = csFactory.create(n, 2)
    for (i <- 0 until n) {
      coords.setOrdinate(i, 0, input.readDouble)
      coords.setOrdinate(i, 1, input.readDouble)
    }
    coords
  }

  override def write(kryo: Kryo, output: Output, shape: Shape) = {
    output.writeShort(getTypeInt(shape))
    shape match {
      case p: Point =>
        output.writeInt(p.dimensions, true)
        p.coord.foreach(output.writeDouble)
      case m: MBR =>
        output.writeInt(m.dimensions, true)
        m.low.coord.foreach(output.writeDouble)
        m.high.coord.foreach(output.writeDouble)
      case c: Circle =>
        output.writeInt(c.dimensions, true)
        c.center.coord.foreach(output.writeDouble)
        output.writeDouble(c.radius)
      case poly: Polygon =>
        val content = poly.content
        writeCoordSequence(output, content.getExteriorRing.getCoordinateSequence)
        val n = content.getNumInteriorRing
        output.writeInt(n, true)
        for (i <- 0 until n) writeCoordSequence(output, content.getInteriorRingN(i).getCoordinateSequence)
      case ls: LineSegment =>
        ls.start.coord.foreach(output.writeDouble)
        ls.end.coord.foreach(output.writeDouble)
    }
  }

  override def read(kryo: Kryo, input: Input, tp: Class[Shape]): Shape = {
    val type_int = input.readShort()
    if (type_int == 0) {
      val dim = input.readInt(true)
      val coords = Array.ofDim[Double](dim)
      for (i <- 0 until dim) coords(i) = input.readDouble()
      Point(coords)
    } else if (type_int == 1) {
      val dim = input.readInt(true)
      val low = Array.ofDim[Double](dim)
      val high = Array.ofDim[Double](dim)
      for (i <- 0 until dim) low(i) = input.readDouble()
      for (i <- 0 until dim) high(i) = input.readDouble()
      MBR(Point(low), Point(high))
    } else if (type_int == 2) {
      val dim = input.readInt(true)
      val center = Array.ofDim[Double](dim)
      for (i <- 0 until dim) center(i) = input.readDouble()
      Circle(Point(center), input.readDouble())
    } else if (type_int == 3) {
      val exterior_ring = gf.createLinearRing(readCoordSequence(input))
      val num_interior_rings = input.readInt(true)
      if (num_interior_rings == 0) Polygon(gf.createPolygon(exterior_ring))
      else {
        val interior_rings = Array.ofDim[LinearRing](num_interior_rings)
        for (i <- 0 until num_interior_rings) interior_rings(i) = gf.createLinearRing(readCoordSequence(input))
        Polygon(gf.createPolygon(exterior_ring, interior_rings))
      }
    } else if (type_int == 4) {
      val start = Array.ofDim[Double](2)
      val end = Array.ofDim[Double](2)
      for (i <- 0 until 2) start(i) = input.readDouble()
      for (i <- 0 until 2) end(i) = input.readDouble()
      LineSegment(Point(start), Point(end))
    } else null
  }
}

