/*
 *  Copyright 2016 by Simba Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.sql.spatial

/**
 * Created by dong on 15-5-27.
 */
object Dist{
    def apply(x : Array[Double], y : Array[Double]) : Double = {
        var ans : Double = 0
        for (i <- 0 to x.length - 1)
            ans += (x(i) - y(i)) * (x(i) - y(i))
        Math.sqrt(ans)
    }

    def furthestMBR(a: MBR, b: MBR) : Double = {
        assert(a.low.dimensions == b.low.dimensions)
        var ans = 0.0
        for (i <- 0 to a.low.dimensions - 1) {
            val tmp_left = math.max((a.low.coord(i) - b.low.coord(i)) * (a.low.coord(i) - b.low.coord(i)),
                (a.low.coord(i) - b.high.coord(i)) * (a.low.coord(i) - b.high.coord(i)))
            val tmp_right = math.max((a.high.coord(i) - b.low.coord(i)) * (a.high.coord(i) - b.low.coord(i)),
                (a.high.coord(i) - b.high.coord(i)) * (a.high.coord(i) - b.high.coord(i)))
            ans += Math.max(tmp_left, tmp_right)
        }
        Math.sqrt(ans)
    }

    def furthestpMBR(a: Point, b: MBR) : Double = {
        assert(a.dimensions == b.low.dimensions)
        var ans = 0.0
        for (i <- 0 to a.dimensions - 1) {
            ans += Math.max((a.coord(i) - b.low.coord(i)) * (a.coord(i) - b.low.coord(i)),
                (a.coord(i) - b.high.coord(i)) * (a.coord(i) - b.high.coord(i)))
        }
        Math.sqrt(ans)
    }
}
