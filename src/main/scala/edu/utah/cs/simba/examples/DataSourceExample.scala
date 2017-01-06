package edu.utah.cs.simba.examples

import edu.utah.cs.simba.SimbaContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gefei on 17-1-6.
  */
object DataSourceExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GeoJsonRelation").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val simbaContext = new SimbaContext(sc)
    val df1 = simbaContext.read.format("edu.utah.cs.simba.execution.datasources")
      .option("type", "geojson").load("./src/main/resources/example.geo.json")
    df1.show()
    val df2 = simbaContext.read.format("edu.utah.cs.simba.execution.datasources")
      .option("type", "osm").load("./src/main/resources/example.osm")
    df2.show()
    val df3 = simbaContext.read.format("edu.utah.cs.simba.execution.datasources")
      .option("type", "shp").load("./src/main/resources/example.shp")
    df3.show()
  }
}
