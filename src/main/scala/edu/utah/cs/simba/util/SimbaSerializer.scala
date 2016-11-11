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

package edu.utah.cs.simba.util

import java.nio.ByteBuffer
import java.util.{HashMap => JavaHashMap}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.twitter.chill.ResourcePool
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer.{KryoSerializer, SerializerInstance}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.util.MutablePair

import edu.utah.cs.simba.index._
import edu.utah.cs.simba.spatial._

import scala.reflect.ClassTag

/**
  * Created by dongx on 11/11/16.
  */
private[simba] class SimbaSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newKryo(): Kryo = {
    val kryo = super.newKryo()
    kryo.setRegistrationRequired(false)
    kryo.register(classOf[MutablePair[_, _]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericMutableRow])
    kryo.register(classOf[java.math.BigDecimal], new JavaBigDecimalSerializer)
    kryo.register(classOf[BigDecimal], new ScalaBigDecimalSerializer)

    kryo.register(classOf[Decimal])
    kryo.register(classOf[JavaHashMap[_, _]])

    kryo.register(classOf[Point])
    kryo.register(classOf[MBR])
    kryo.register(classOf[RTreeNode])
    kryo.register(classOf[RTreeEntry])
    kryo.register(classOf[RTree])
    kryo.register(classOf[java.util.TreeMap[_, _]])
    kryo.register(classOf[java.util.HashMap[_, _]])
    kryo.register(classOf[TreeMapIndex[_]])
    kryo.register(classOf[HashMapIndex[_]])
    kryo.register(classOf[Treap[_]])

    kryo.setReferences(false)
    kryo
  }
}

private[simba] class KryoResourcePool(size: Int)
  extends ResourcePool[SerializerInstance](size) {

  val ser: SimbaSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SimbaSerializer(sparkConf)
  }

  def newInstance(): SerializerInstance = ser.newInstance()
}

private[simba] object SimbaSerializer {
  @transient lazy val resourcePool = new KryoResourcePool(30)

  private[this] def acquireRelease[O](fn: SerializerInstance => O): O = {
    val kryo = resourcePool.borrow
    try {
      fn(kryo)
    } finally {
      resourcePool.release(kryo)
    }
  }

  def serialize[T: ClassTag](o: T): Array[Byte] =
    acquireRelease { k =>
      k.serialize(o).array()
    }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T =
    acquireRelease { k =>
      k.deserialize[T](ByteBuffer.wrap(bytes))
    }
}

private[simba] class JavaBigDecimalSerializer extends Serializer[java.math.BigDecimal] {
  def write(kryo: Kryo, output: Output, bd: java.math.BigDecimal) {
    output.writeString(bd.toString)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[java.math.BigDecimal]): java.math.BigDecimal = {
    new java.math.BigDecimal(input.readString())
  }
}

private[simba] class ScalaBigDecimalSerializer extends Serializer[BigDecimal] {
  def write(kryo: Kryo, output: Output, bd: BigDecimal) {
    output.writeString(bd.toString)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[BigDecimal]): BigDecimal = {
    new java.math.BigDecimal(input.readString())
  }
}
