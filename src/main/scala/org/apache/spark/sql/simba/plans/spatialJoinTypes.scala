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

package org.apache.spark.sql.simba.plans

/**
  * Created by dongx on 11/13/2016.
  */
object SpatialJoinType {
  def apply(typ: String): SpatialJoinType = typ.toLowerCase.replace("_", "") match {
    case "knn" => KNNJoin
    case "distance" => DistanceJoin
    case "zknn" => ZKNNJoin
    case _ =>
      val supported = Seq("knn", "distance", "zknn")

      throw new IllegalArgumentException(s"Unsupported spatial join type '$typ'. " +
        "Supported spatial join types include: " + supported.mkString("'", "', '", "'") + ".")
  }
}

sealed abstract class SpatialJoinType

case object KNNJoin extends SpatialJoinType

case object DistanceJoin extends SpatialJoinType

case object ZKNNJoin extends SpatialJoinType
