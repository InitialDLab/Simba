package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.SimbaSession

/**
  * Created by dongx on 3/7/2017.
  */
object BasicSpatialOps {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("SparkSessionForSimba")
      .config("simba.join.partitions", "20")
      .getOrCreate()

    runRangeQuery(simbaSession)
    runKnnQuery(simbaSession)
    runJoinQUery(simbaSession)
    simbaSession.stop()
  }

  private def runKnnQuery(simba: SimbaSession): Unit = {

    import simba.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()

    import simba.simbaImplicits._
    caseClassDS.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)

  }

  private def runRangeQuery(simba: SimbaSession): Unit = {

    import simba.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()

    import simba.simbaImplicits._
    caseClassDS.range(Array("x", "y"),Array(1.0, 1.0),Array(3.0, 3.0)).show(10)

  }

  private def runJoinQUery(simba: SimbaSession): Unit = {

    import simba.implicits._

    val DS1 = (0 until 10000).map(x => PointData(x, x + 1, x + 2, x.toString)).toDS
    val DS2 = (0 until 10000).map(x => PointData(x, x, x + 1, x.toString)).toDS

    import simba.simbaImplicits._

    DS1.knnJoin(DS2, Array("x", "y"),Array("x", "y"), 3).show()

    DS1.distanceJoin(DS2, Array("x", "y"),Array("x", "y"), 3).show()

  }
}
