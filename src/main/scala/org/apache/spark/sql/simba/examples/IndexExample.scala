package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.{RTreeType, TreapType}

/**
  * Created by dongx on 3/7/2017.
  */
object IndexExample {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("IndexExample")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    buildIndex(simbaSession)
    useIndex1(simbaSession)
    useIndex2(simbaSession)
    simbaSession.stop()
  }

  private def buildIndex(simba: SimbaSession): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS

    datapoints.createOrReplaceTempView("a")

    simba.indexTable("a", RTreeType, "testqtree",  Array("x", "y") )

    simba.showIndex("a")
  }

  private def useIndex1(simba: SimbaSession): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()

    datapoints.createOrReplaceTempView("b")

    simba.indexTable("b", RTreeType, "RtreeForData",  Array("x", "y") )

    simba.showIndex("b")

    val res = simba.sql("SELECT * FROM b")
    res.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)

  }

  private def useIndex2(simba: SimbaSession): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()

    datapoints.createOrReplaceTempView("b")

    simba.indexTable("b", RTreeType, "RtreeForData",  Array("x", "y") )

    simba.showIndex("b")

    simba.sql("SELECT * FROM b where b.x >1 and b.y<=2").show(5)

  }

  private def useIndex3(simba: SimbaSession): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(0.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()

    import simba.simbaImplicits._

    datapoints.index(TreapType, "indexForOneTable",  Array("x"))

    datapoints.range(Array("x"),Array(1.0),Array(2.0)).show(4)
  }
}
