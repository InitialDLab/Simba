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

package org.apache.spark.sql.simba.spatial

/**
 * Created by dong on 1/15/16.
 * Distance Utilities
 */
object Dist {
  def furthest(a: Point, b: MBR) : Double = {
    require(a.coord.length == b.low.coord.length)
    var ans = 0.0
    for (i <- a.coord.indices) {
      ans += Math.max((a.coord(i) - b.low.coord(i)) * (a.coord(i) - b.low.coord(i)),
        (a.coord(i) - b.high.coord(i)) * (a.coord(i) - b.high.coord(i)))
    }
    Math.sqrt(ans)
  }
}
