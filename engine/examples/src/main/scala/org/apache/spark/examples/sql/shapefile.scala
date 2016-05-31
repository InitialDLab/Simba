package org.apache.spark.examples.sql

import scala.annotation.tailrec

import com.vividsolutions.jts.geom._

import java.nio.channels.FileChannel
import java.nio.{ByteBuffer,ByteOrder}
import java.io.RandomAccessFile

package object shapefile {

  import Implicits._

  case class Record(id: Int, g: Geometry)

  class ShapefileParser extends ShapefileStructure with TypeMapper

  object Parser {
    def apply(f: String)(implicit g: GeometryFactory) = new ShapefileParser().parse(f)(g)
  }

  trait TypeMapper {
    val types = Seq(NullParser,PointParser,PolyLineParser,PolygonParser,MultiPointParser)
    def getType(n: Int):Either[String,ShapeParser] = types.filter(_.shapeType == n).headOption match {
      case Some(t) => Right(t)
      case None => Left(s"Failed to fine a valid type for $n")
    }
  }

  trait ShapefileStructure { this: TypeMapper =>

    def parseMagicNumber(b: ByteBuffer) {
      if (b.big.getInt != 9994)
        sys.error("Expected 9994 (shapefile magic number)")
    }

    def parseVersion(b: ByteBuffer) {
      if (b.little.getInt != 1000)
        sys.error("Expected 1000 (invalid version)")
    }

    def parseShapeType(b: ByteBuffer):ShapeParser = getType(b.little.getInt) match {
      case Right(t) => t
      case Left(t) => sys.error(t)
    }

    def parseHeader(b: ByteBuffer):ShapeParser = {
      parseMagicNumber(b.big)
      b.skip(20) // skip 5 ints
      b.getInt // ignore length
      parseVersion(b)

      val shapeParser = parseShapeType(b)
      b.skip(8*8) // skip bbox (x,y,z,m)

      shapeParser
    }

    def parse(fileName: String)(implicit g: GeometryFactory):Seq[Record] = {
      val inChannel = new RandomAccessFile(fileName, "r").getChannel()
      val buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size())

      val parser = parseHeader(buffer)

      var gs = Seq.empty[Record]
      while(buffer.position < buffer.limit)
        gs = gs :+ parser.parseRecord(buffer)

      inChannel.close()
      gs
    }
  }

  trait ShapeParser {
    val shapeType: Int

    def apply(b: ByteBuffer)(implicit g: GeometryFactory): Geometry
    def parse(b: ByteBuffer)(implicit g: GeometryFactory): Geometry = apply(b)

    def parseRecord(b: ByteBuffer)(implicit g: GeometryFactory): Record = {
      val rid = b.big.getInt
      val clen = b.big.getInt
      val shpTyp = b.little.getInt

      shpTyp match {
        case NullParser.shapeType => Record(rid, NullParser(b))
        case t if t == shapeType => Record(rid, parse(b))
        case t => sys.error(s"Expected shape type ${shapeType}, got $t")
      }
    }
  }

  object NullParser extends ShapeParser {
    lazy val shapeType = 0

    def apply(b: ByteBuffer)(implicit g: GeometryFactory) = {
      g.createPoint(null:Coordinate)
    }
  }

  object PointParser extends ShapeParser {
    lazy val shapeType = 1

    def apply(b: ByteBuffer)(implicit g: GeometryFactory) = {
      g.createPoint(new Coordinate(b.little.getDouble, b.getDouble))
    }
  }

  object MultiPointParser extends ShapeParser {
    lazy val shapeType = 8

    def apply(b: ByteBuffer)(implicit g: GeometryFactory) = {
      b.skipBbox
      g.createMultiPoint(
        Array.ofDim[Point](b.little.getInt) map { _ => PointParser(b) })
    }
  }

  trait PolyThingParser extends ShapeParser {
    def parseCoordinate(b: ByteBuffer) = new Coordinate(b.little.getDouble, b.getDouble)

    // rewrite as fold?
    @tailrec
    final def take[T](v: Seq[T], s: Seq[Int], a: Seq[Seq[T]]):Seq[Seq[T]] = s match {
      case Nil => a
      case Seq(x, xs@_*) => v.splitAt(x) match {
        case (n,r) => take(r, xs, r +: a)
      }
    }

    type A

    def pointsToInnerGeom(a: Seq[Coordinate])(implicit g: GeometryFactory):A
    def buildCollection(a: Seq[A])(implicit g: GeometryFactory):Geometry

    def apply(b: ByteBuffer)(implicit g: GeometryFactory) = {
      b.skipBbox
      val nParts = b.little.getInt
      val nPts = b.little.getInt
      val terminals = (0 until nParts).map { _ => b.getInt }
      val tOffsets = terminals.zip(0 +: terminals).map { case (a,b) => a - b }

      val points = (0 until nPts) map { _ => parseCoordinate(b) }
      val strings = take(points, tOffsets, Seq.empty) map { c => pointsToInnerGeom(c) }

      buildCollection(strings)
    }
  }

  object PolyLineParser extends PolyThingParser {
    lazy val shapeType = 3

    type A = LineString
    def pointsToInnerGeom(a: Seq[Coordinate])(implicit g: GeometryFactory) = g.createLineString(a.toArray)
    def buildCollection(a: Seq[LineString])(implicit g: GeometryFactory) = g.createMultiLineString(a.toArray)
  }

  object PolygonParser extends PolyThingParser {
    lazy val shapeType = 5

    type A = LinearRing
    def pointsToInnerGeom(a: Seq[Coordinate])(implicit g: GeometryFactory) =
      g.createLinearRing((a :+ a.head).toArray)

    def buildCollection(a: Seq[LinearRing])(implicit g: GeometryFactory) =
      g.createPolygon(a.head, a.tail.toArray)
  }
}

object Implicits {
  class ExtendedByteBuffer(val b: ByteBuffer) extends AnyVal {
    def little = b.order(ByteOrder.LITTLE_ENDIAN)
    def big = b.order(ByteOrder.BIG_ENDIAN)
    def skip(nBytes: Int) = b.position(b.position + nBytes)
    def skipBbox = b.skip(32) // skip 4 doubles (32 bytes)
  }
  implicit def extendByteBuffer(b: ByteBuffer):ExtendedByteBuffer = new ExtendedByteBuffer(b)
}
