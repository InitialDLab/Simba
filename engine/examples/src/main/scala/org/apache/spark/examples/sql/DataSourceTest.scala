/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by gefei on 16-7-25.
  */
object DataSourceTest {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf()
        .setAppName("DataSourceTest")
        .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("org.apache.spark.sql.execution.datasources.spatial")
          .option("type", "geojson")
      .load("./examples/src/main/resources/example.geo.json")
    df.show()
  }
}
