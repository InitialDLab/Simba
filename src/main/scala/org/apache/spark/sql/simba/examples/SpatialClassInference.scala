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

package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.spatial.Point

/**
  * Created by dongx on 3/16/17.
  */
object SpatialClassInference {
  case class PointData(p: Point, payload: Int)

  def main(args: Array[String]) = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("SpatialClassInference")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    import simbaSession.implicits._
    import simbaSession.simbaImplicits._
    val ps = (0 until 10000).map(x => PointData(Point(Array(x.toDouble, x.toDouble)), x + 1)).toDS
    ps.knn("p", Array(1.0, 1.0), 4).show()
    ps.range("p", Array(1.0, 2.0), Array(4.0, 5.0)).show()
  }
}
