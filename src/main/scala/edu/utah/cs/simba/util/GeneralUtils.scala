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

import java.io.{IOException, InputStream, OutputStream}
import java.util

import org.apache.spark.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}

import scala.reflect.{ClassTag, classTag}
import scala.util.Random
import scala.util.control.NonFatal

/**
  * Created by dongx on 11/11/16.
  * Sampling and searching utils
  */
private[simba] object GeneralUtils extends Logging {
  def makeBinarySearch[K : Ordering : ClassTag] : (Array[K], K) => Int = {
    // For primitive keys, we can use the natural ordering. Otherwise, use the Ordering comparator.
    classTag[K] match {
      case ClassTag.Float =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Float]], x.asInstanceOf[Float])
      case ClassTag.Double =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Double]], x.asInstanceOf[Double])
      case ClassTag.Byte =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Byte]], x.asInstanceOf[Byte])
      case ClassTag.Char =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Char]], x.asInstanceOf[Char])
      case ClassTag.Short =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Short]], x.asInstanceOf[Short])
      case ClassTag.Int =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Int]], x.asInstanceOf[Int])
      case ClassTag.Long =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Long]], x.asInstanceOf[Long])
      case _ =>
        val comparator = implicitly[Ordering[K]].asInstanceOf[java.util.Comparator[Any]]
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[AnyRef]], x, comparator)
    }
  }

  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
    f: SerializationStream => Unit): Unit = {
    val osWrapper = ser.serializeStream(new OutputStream {
      override def write(b: Int): Unit = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
    f: DeserializationStream => Unit): Unit = {
    val isWrapper = ser.deserializeStream(new InputStream {
      override def read(): Int = is.read()
      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  def reservoirSampleAndCount[T: ClassTag](input: Iterator[T], k: Int, seed: Long = Random.nextLong())
  : (Array[T], Long) = {
    val reservoir = new Array[T](k)
    // Put the first k elements in the reservoir.
    var i = 0
    while (i < k && input.hasNext) {
      val item = input.next()
      reservoir(i) = item
      i += 1
    }

    // If we have consumed all the elements, return them. Otherwise do the replacement.
    if (i < k) {
      // If input size < k, trim the array to return only an array of input size.
      val trimReservoir = new Array[T](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      (trimReservoir, i)
    } else {
      // If input size > k, continue the sampling process.
      var l = i.toLong
      val rand = new XORShiftRandom(seed)
      while (input.hasNext) {
        val item = input.next()
        val replacementIndex = (rand.nextDouble() * l).toLong
        if (replacementIndex < k) {
          reservoir(replacementIndex.toInt) = item
        }
        l += 1
      }
      (reservoir, l)
    }
  }
}
