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

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.partitioner.{QuadTreePartitioner, RangePartition}
import org.apache.spark.sql.types.{DoubleType, IntegerType, NumericType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.IndexRDD
import org.apache.spark.sql.spatial.Point

/**
  * Created by gefei on 16-7-30.
  */
private[sql] case class QuadTreeIndexedRelation(
     output: Seq[Attribute],
     child: SparkPlan,
     table_name: Option[String],
     column_keys: List[Attribute],
     index_name: String)(var _indexedRDD: IndexRDD = null,
                         var global_index: QuadTree = null)
  extends IndexedRelation with MultiInstanceRelation {
  private def checkKeys: Boolean = {
    for (i <- column_keys.indices)
      if (!(column_keys(i).dataType.isInstanceOf[DoubleType] ||
        column_keys(i).dataType.isInstanceOf[IntegerType])) {
        return false
      }
    true
  }
  require(checkKeys)

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[sql] def buildIndex(): Unit = {
    val numShufflePartitions = child.sqlContext.conf.numShufflePartitions
    val sampleRate = child.sqlContext.conf.sampleRate
    val tranferThreshold = child.sqlContext.conf.transferThreshold

    val dataRDD = child.execute().map(row => {
      val now = column_keys.map(x =>
        BindReferences.bindReference(x, child.output).eval(row).asInstanceOf[Number].doubleValue()
      ).toArray
      (new Point(now), row)
    })

    val dimension = column_keys.length
    val (partitionedRDD, _, global_qtree) = QuadTreePartitioner(dataRDD, dimension,
      numShufflePartitions, sampleRate, tranferThreshold)

    val indexed = partitionedRDD.mapPartitions { iter =>
      val data = iter.toArray
      val index: QuadTree =
        if (data.length > 0) QuadTree(data.map(_._1).zipWithIndex)
        else null
      Array(IPartition(data.map(_._2), index)).iterator
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    indexed.setName(table_name.map(name => s"$name $index_name").getOrElse(child.toString))
    _indexedRDD = indexed
    global_index = global_qtree
  }

  override def newInstance(): IndexedRelation = {
    new QuadTreeIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD)
      .asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    new QuadTreeIndexedRelation(new_output, child, table_name,
      column_keys, index_name)(_indexedRDD, global_index)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )

}
