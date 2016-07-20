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
package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, PointWrapperExpression}
import org.apache.spark.sql.spatial.Point
/**
  * Created by zhongpu on 16-7-19.
  */
object FetchPointUtils {

  def getFromRow(row: InternalRow, point: Expression): Point = {
    val point_coord = point match {
      case p: PointWrapperExpression => p.points.toArray.map(_.eval(row).
        asInstanceOf[Number].doubleValue())
      case e => e.eval(row).asInstanceOf[GenericInternalRow].values(0)
        .asInstanceOf[GenericArrayData].array.map(x => x.asInstanceOf[Double])
    }
    new Point(point_coord)
  }
}
