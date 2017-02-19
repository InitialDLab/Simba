package edu.utah.cs.simba.execution.datasources

import edu.utah.cs.simba.{ShapeType, SimbaFunSuite}
import org.apache.spark.sql.types.{StructField, StructType}

class OSMRelationSuite extends SimbaFunSuite {
  test("OSM file test") {
    val df = simbaContext.read.format("edu.utah.cs.simba.execution.datasources")
      .option("type", "osm").load("./src/main/resources/example.osm")
    val expectedSchema = StructType(
      StructField("position", ShapeType, true) :: Nil
    )
    assert(df.schema == expectedSchema)
    assert(df.collect().length == 6)
  }
}
