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

package org.apache.spark.sql.simba

import java.util.Properties

import scala.collection.immutable
import scala.collection.JavaConverters._

/**
  * Created by dongx on 11/11/16.
  * Simba Configuration
  */
private[simba] object SimbaConf {
  private val simbaConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, SimbaConfEntry[_]]())

  private[simba] class SimbaConfEntry[T] private(
      val key: String,
      val defaultValue: Option[T],
      val valueConverter: String => T,
      val stringConverter: T => String,
      val doc: String,
      val isPublic: Boolean) {
    def defaultValueString: String = defaultValue.map(stringConverter).getOrElse("<undefined>")

    override def toString: String = {
      s"SimbaConfEntry(key = $key, defaultValue=$defaultValueString, doc=$doc, isPublic = $isPublic)"
    }
  }

  private[simba] object SimbaConfEntry {
    private def apply[T](key: String, defaultValue: Option[T], valueConverter: String => T,
                         stringConverter: T => String, doc: String, isPublic: Boolean): SimbaConfEntry[T] =
      simbaConfEntries.synchronized {
        if (simbaConfEntries.containsKey(key)) {
          throw new IllegalArgumentException(s"Duplicate SimbaConfEntry. $key has been registered")
        }
        val entry =
          new SimbaConfEntry[T](key, defaultValue, valueConverter, stringConverter, doc, isPublic)
        simbaConfEntries.put(key, entry)
        entry
      }

    def intConf(key: String, defaultValue: Option[Int] = None,
                doc: String = "", isPublic: Boolean = true): SimbaConfEntry[Int] =
      SimbaConfEntry(key, defaultValue, { v =>
        try {
          v.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be int, but was $v")
        }
      }, _.toString, doc, isPublic)

    def longConf(key: String, defaultValue: Option[Long] = None,
                 doc: String = "", isPublic: Boolean = true): SimbaConfEntry[Long] =
      SimbaConfEntry(key, defaultValue, { v =>
        try {
          v.toLong
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be long, but was $v")
        }
      }, _.toString, doc, isPublic)

    def doubleConf(key: String, defaultValue: Option[Double] = None,
                   doc: String = "", isPublic: Boolean = true): SimbaConfEntry[Double] =
      SimbaConfEntry(key, defaultValue, { v =>
        try {
          v.toDouble
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be double, but was $v")
        }
      }, _.toString, doc, isPublic)

    def booleanConf(key: String, defaultValue: Option[Boolean] = None,
                    doc: String = "", isPublic: Boolean = true): SimbaConfEntry[Boolean] =
      SimbaConfEntry(key, defaultValue, { v =>
        try {
          v.toBoolean
        } catch {
          case _: IllegalArgumentException =>
            throw new IllegalArgumentException(s"$key should be boolean, but was $v")
        }
      }, _.toString, doc, isPublic)

    def stringConf(key: String, defaultValue: Option[String] = None,
                   doc: String = "", isPublic: Boolean = true): SimbaConfEntry[String] =
      SimbaConfEntry(key, defaultValue, v => v, v => v, doc, isPublic)
  }

  import SimbaConfEntry._

  val INDEX_PARTITIONS = intConf("simba.index.partitions", defaultValue = Some(200))
  val JOIN_PARTITIONS = intConf("simba.join.partitions", defaultValue = Some(200))
  val DISTANCE_JION = stringConf("simba.join.distanceJoin", defaultValue = Some("DJSpark"))
  val KNN_JOIN = stringConf("simba.join.knnJoin", defaultValue = Some("RKJSpark"))

  // RTree Parameters
  val MAX_ENTRIES_PER_NODE = intConf("simba.rtree.maxEntriesPerNode", defaultValue = Some(25))

  // zKNN Join Parameters
  val ZKNN_SHIFT_TIMES = intConf("simba.join.zknn.shiftTimes", defaultValue = Some(2))

  // Voronoi KNN Join Parameters
  val VORONOI_PIVOTSET_SIZE = intConf("simba.join.voronoi.pivotSetSize", defaultValue = Some(10))
  val THETA_BOOST = intConf("simba.join.rkj.thetaBoost", defaultValue = Some(16))
  val SAMPLE_RATE = doubleConf("simba.sampleRate", defaultValue = Some(0.01))

  // Partitioner Parameter
  val PARTITION_METHOD = stringConf("simba.partition.method", defaultValue = Some("STRPartitioner"))
  val TRANSFER_THRESHOLD = longConf("simba.transferThreshold", defaultValue =
    Some(800 * 1024 * 1024))

  val INDEX_SELECTIVITY_ENABLE =
    booleanConf("simba.index.selectivityEnable",
      defaultValue = Some(false),
      doc = "When true, If the selectivity of the predicate is higher than a " +
        "user-defined threshold, Simba will choose to scan the partition " +
        "rather than leveraging local (RTree) indexes for Range query. " +
        "The default value is false.")

  val INDEX_SELECTIVITY_THRESHOLD =
    doubleConf("simba.index.selectivityThreshold",
      defaultValue = Some(0.8),
      doc = "This only works when INDEX_SELECTIVITY_ENABLE is true. " +
        "The user-defined local (RTree) index selectivity threshold is between 0.0 to 1.0. " +
        "The default value is 0.8.")

  val INDEX_SELECTIVITY_LEVEL =
    intConf("simba.index.selectivityLevel",
      defaultValue = Some(1),
      doc = "This only works when INDEX_SELECTIVITY_ENABLE is true." +
        "Simab considers predicates selectivity from this level, default root level (1)")

  // Threshold determine where rtree index using local index or brute force filter
  val INDEX_SIZE_THRESHOLD = intConf("simba.index.sizeThreshold", defaultValue = Some(1000))
}

private[simba] class SimbaConf extends Serializable {
  import SimbaConf._

  @transient protected[simba] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  private[simba] def indexPartitions: Int = getConf(INDEX_PARTITIONS)
  private[simba] def joinPartitions: Int = getConf(JOIN_PARTITIONS)
  private[simba] def distanceJoin: String = getConf(DISTANCE_JION)
  private[simba] def knnJoin: String = getConf(KNN_JOIN)
  private[simba] def partitionMethod: String = getConf(PARTITION_METHOD)
  private[simba] def maxEntriesPerNode: Int = getConf(MAX_ENTRIES_PER_NODE)
  private[simba] def zknnShiftTimes: Int = getConf(ZKNN_SHIFT_TIMES)
  private[simba] def voronoiPivotSetSize: Int = getConf(VORONOI_PIVOTSET_SIZE)
  private[simba] def thetaBoost: Int = getConf(THETA_BOOST)
  private[simba] def sampleRate: Double = getConf(SAMPLE_RATE)
  private[simba] def indexSelectivityEnable : Boolean = getConf(INDEX_SELECTIVITY_ENABLE)
  private[simba] def indexSelectivityThreshold : Double = getConf(INDEX_SELECTIVITY_THRESHOLD)
  private[simba] def indexSelectivityLevel : Int = getConf(INDEX_SELECTIVITY_LEVEL)
  private[simba] def indexSizeThreshold: Int = getConf(INDEX_SIZE_THRESHOLD)
  private[simba] def transferThreshold: Long = getConf(TRANSFER_THRESHOLD)

  def setConf(props: Properties): Unit = settings.synchronized {
    props.asScala.foreach { case (k, v) => setConfString(k, v) }
  }

  def setConfString(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    val entry = simbaConfEntries.get(key)
    if (entry != null) {
      // Only verify configs in the SimbaConf object
      entry.valueConverter(value)
    }
    settings.put(key, value)
  }

  def setConf[T](entry: SimbaConfEntry[T], value: T): Unit = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    require(simbaConfEntries.get(entry.key) == entry, s"$entry is not registered")
    settings.put(entry.key, entry.stringConverter(value))
  }

  def getConfString(key: String): String = {
    Option(settings.get(key)).
      orElse {
        // Try to use the default value
        Option(simbaConfEntries.get(key)).map(_.defaultValueString)
      }.
      getOrElse(throw new NoSuchElementException(key))
  }

  def getConf[T](entry: SimbaConfEntry[T], defaultValue: T): T = {
    require(simbaConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).getOrElse(defaultValue)
  }

  def getConf[T](entry: SimbaConfEntry[T]): T = {
    require(simbaConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).orElse(entry.defaultValue).
      getOrElse(throw new NoSuchElementException(entry.key))
  }

  def getConfString(key: String, defaultValue: String): String = {
    val entry = simbaConfEntries.get(key)
    if (entry != null && defaultValue != "<undefined>") {
      // Only verify configs in the SimbaConf object
      entry.valueConverter(defaultValue)
    }
    Option(settings.get(key)).getOrElse(defaultValue)
  }


  def getAllConfs: immutable.Map[String, String] =
    settings.synchronized { settings.asScala.toMap }


  def getAllDefinedConfs: Seq[(String, String, String)] = simbaConfEntries.synchronized {
    simbaConfEntries.values.asScala.filter(_.isPublic).map { entry =>
      (entry.key, entry.defaultValueString, entry.doc)
    }.toSeq
  }

  private[simba] def unsetConf(key: String): Unit = {
    settings.remove(key)
  }

  private[simba] def unsetConf(entry: SimbaConfEntry[_]): Unit = {
    settings.remove(entry.key)
  }

  private[simba] def clear(): Unit = {
    settings.clear()
  }
}
