package edu.utah.cs.simba.index

import edu.utah.cs.simba.spatial.{Circle, MBR, Point}
import org.scalatest.FunSuite

/**
  * Created by gefei on 2016/11/14.
  */
class QuadTreeSuite extends FunSuite{
  var entries = new Array[(Point, Int)](221)
  var cnt = 0
  for (i <- -10 to 10){
    for(j <- -10 to 10){
      if(Math.abs(i) + Math.abs(j) <= 10) {
        entries(cnt) = (new Point(Array(i, j)), i + j)
        cnt = cnt + 1
      }
    }
  }

  val quadTree = QuadTree.apply(entries)

  test("QuadTree: range, simple") {
    val A = Point(Array(0.0, 0.0))
    val B = Point(Array(9.0, 9.0))
    val mbr = MBR(A, B)
    val range = quadTree.range(mbr)

    for(x <-range){
      assert(mbr.intersects(x._1))
    }

    var count = 0
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          if(i >= 0 && j >= 0 && i <= 9 && j <= 9)
            count = count + 1
        }
      }
    }
    assert(range.length == count)
  }

  test("QuadTree: range, complex"){
    val A = Point(Array(0.0, 0.0))
    val B = Point(Array(9.0, 9.0))
    val mbr = MBR(A, B)

    val ans = quadTree.range(mbr)
    for(x <-ans){
      assert(mbr.intersects(x._1))
    }
    var count = 0
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          if(i >= 0 && j >= 0 && i <= 9 && j <= 9)
            count = count + 1
        }
      }
    }
    assert(ans.length == count)
  }

  test("RTree: circleRange"){
    val center = Point(Array(6.0, 6.0))
    val  radius = 6.0
    val  circle = Circle(center, radius)
    val range = quadTree.circleRange(center, radius)

    for(x <-range){
      assert(circle.intersects(x._1))
    }

    var count = 0
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          if((i - 6) * (i - 6) + (j - 6) * (j - 6) <= 36)
            count = count + 1
        }
      }
    }
    assert(range.length == count)
  }
}
