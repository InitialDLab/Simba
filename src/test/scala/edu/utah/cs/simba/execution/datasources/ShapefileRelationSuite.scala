package edu.utah.cs.simba.execution.datasources

import edu.utah.cs.simba.{ShapeType, SimbaFunSuite}
import org.apache.spark.sql.types.{StructField, StructType}

class ShapefileRelationSuite extends SimbaFunSuite {
  test("Shapefile test") {
    val df = simbaContext.read.format("edu.utah.cs.simba.execution.datasources")
      .option("type", "shp").load("./src/main/resources/example.shp")
    val expectedSchema = StructType(
      StructField("shape", ShapeType, true) :: Nil
    )
    assert(df.schema == expectedSchema)
    assert(df.collect().length == 382)
  }
}
