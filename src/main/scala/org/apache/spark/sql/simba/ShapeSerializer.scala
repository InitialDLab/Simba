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

package org.apache.spark.sql.simba

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import java.io._

import org.apache.spark.sql.simba.spatial._
import org.apache.spark.sql.simba.util.KryoShapeSerializer

/**
  * Created by dongx on 11/15/16.
  */
object ShapeSerializer {
  private[simba] val kryo = new Kryo()

  kryo.register(classOf[Shape], new KryoShapeSerializer)
  kryo.register(classOf[Point], new KryoShapeSerializer)
  kryo.register(classOf[MBR], new KryoShapeSerializer)
  kryo.register(classOf[Polygon], new KryoShapeSerializer)
  kryo.register(classOf[Circle], new KryoShapeSerializer)
  kryo.register(classOf[LineSegment], new KryoShapeSerializer)
  kryo.addDefaultSerializer(classOf[Shape], new KryoShapeSerializer)
  kryo.setReferences(false)

  def deserialize(data: Array[Byte]): Shape = {
    val in = new ByteArrayInputStream(data)
    val input = new Input(in)
    val res = kryo.readObject(input, classOf[Shape])
    input.close()
    res
  }

  def serialize(o: Shape): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val output = new Output(out)
    kryo.writeObject(output, o)
    output.close()
    out.toByteArray
  }
}

class ShapeSerializer {

}