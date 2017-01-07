/*
 * Copyright 2016 by Simba Project
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

package edu.utah.cs.simba

import com.vividsolutions.jts.geom.{Polygon => JTSPolygon}
import org.apache.spark.sql.types._
import edu.utah.cs.simba.spatial.{Polygon, Shape}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}

/**
  * Created by dongx on 11/10/16.
  */
private[simba] class ShapeType extends UserDefinedType[Shape] {
  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)

  override def serialize(s: Any): Any = {
    s match {
      case o: Shape =>
        new GenericArrayData(ShapeSerializer.serialize(o))
      case g: JTSPolygon => // An ugly hack here
        val pol = Polygon(g)
        new GenericArrayData(ShapeSerializer.serialize(pol))
    }
  }

  override def userClass: Class[Shape] = classOf[Shape]

  override def deserialize(datum: Any): Shape = {
    datum match {
      case values: ArrayData =>
        ShapeSerializer.deserialize(values.toByteArray)
    }
  }
}

case object ShapeType extends ShapeType
