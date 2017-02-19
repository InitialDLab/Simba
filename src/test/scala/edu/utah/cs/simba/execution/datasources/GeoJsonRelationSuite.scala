package edu.utah.cs.simba.execution.datasources

import edu.utah.cs.simba.{ShapeType, SimbaFunSuite}
import org.apache.spark.sql.types.{StructField, StructType}

class GeoJsonRelationSuite extends SimbaFunSuite {
  test("GeoJson file test") {
    val df = simbaContext.read.format("edu.utah.cs.simba.execution.datasources")
      .option("type", "geojson").load("./src/main/resources/example.geo.json")
    val expectedSchema = StructType(
      StructField("shape", ShapeType, true) :: Nil
    )
    assert(df.schema == expectedSchema)
    assert(df.collect().length == 3)
  }
}


