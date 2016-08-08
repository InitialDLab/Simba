/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.spark.sql.index

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.partitioner.{KDTreePartitioner, QuadTreePartitioner, STRPartition}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.FetchPointUtils
import org.apache.spark.storage.StorageLevel

/**
  * Created by gefei on 16-6-21.
  */
private[sql] case class RTreeIndexedRelation(
  output: Seq[Attribute],
  child: SparkPlan,
  table_name: Option[String],
  column_keys: List[Attribute],
  index_name: String)(var _indexedRDD: IndexRDD = null,
                      var global_rtree: RTree = null)
  extends IndexedRelation with MultiInstanceRelation {

  var isPoint = false

  private def checkKeys: Boolean = {
    if (column_keys.length > 1) {
      for (i <- column_keys.indices)
        if (!column_keys(i).dataType.isInstanceOf[NumericType]) {
          return false
        }
      true
    } else { // length = 1; we do not support one dimension R-tree
      column_keys.head.dataType match {
        case t: ShapeType =>
          isPoint = true
          true
        case _ => false
      }
    }
  }
  require(checkKeys)

  val dimension = FetchPointUtils.getFromRow(child.execute().first(), column_keys, child, isPoint)
    .coord.length

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[sql] def buildIndex(): Unit = {
    val numShufflePartitions = child.sqlContext.conf.numShufflePartitions
    val maxEntriesPerNode = child.sqlContext.conf.maxEntriesPerNode
    val sampleRate = child.sqlContext.conf.sampleRate
    val transferThreshold = child.sqlContext.conf.transferThreshold
    val dataRDD = child.execute().map(row => {
      (FetchPointUtils.getFromRow(row, column_keys, child, isPoint), row)
    })

    val max_entries_per_node = maxEntriesPerNode
    val (partitionedRDD, mbr_bounds) = child.sqlContext.conf.partitionMethod match {
      case "KDTreeParitioner" => KDTreePartitioner(dataRDD, dimension, numShufflePartitions,
        sampleRate, transferThreshold)
      case "QuadTreePartitioner" =>
        val temp = QuadTreePartitioner(dataRDD, dimension, numShufflePartitions,
              sampleRate, transferThreshold)
        (temp._1, temp._2)
      // only RTree needs max_entries_per_node parameter
      case _ => STRPartition (dataRDD, dimension, numShufflePartitions,
        sampleRate, transferThreshold, max_entries_per_node)// default
    }

    val indexed = partitionedRDD.mapPartitions { iter =>
      val data = iter.toArray
      var index: RTree = null
      if (data.length > 0) index = RTree(data.map(_._1).zipWithIndex, max_entries_per_node)
      Array(IPartition(data.map(_._2), index)).iterator
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val partitionSize = indexed.mapPartitions(iter => iter.map(_.data.length)).collect()

    global_rtree = RTree(mbr_bounds.zip(partitionSize)
      .map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)
    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    _indexedRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new RTreeIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD).asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    RTreeIndexedRelation(new_output, child, table_name,
      column_keys, index_name)(_indexedRDD, global_rtree)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )
}