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
 */

package org.apache.spark.sql.simba.index

import org.apache.spark.sql.simba.partitioner.RangePartition
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.NumericType
import org.apache.spark.storage.StorageLevel

/**
  * Created by dongx on 11/12/2016.
  */
private[simba] case class TreapIndexedRelation(output: Seq[Attribute], child: SparkPlan,
    table_name: Option[String], column_keys: List[Attribute], index_name: String)
    (var _indexedRDD: IndexedRDD = null, var range_bounds: Array[Double] = null)
  extends IndexedRelation with MultiInstanceRelation {

  require(column_keys.length == 1)
  require(column_keys.head.dataType.isInstanceOf[NumericType])
  val numShufflePartitions = simbaSession.sessionState.simbaConf.indexPartitions

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[simba] def buildIndex(): Unit = {
    val dataRDD = child.execute().map(row => {
      val eval_key = BindReferences.bindReference(column_keys.head, child.output).eval(row)
        .asInstanceOf[Double]
      (eval_key, row)
    })

    val (partitionedRDD, tmp_bounds) = RangePartition.rowPartition(dataRDD, numShufflePartitions)
    range_bounds = tmp_bounds
    val indexed = partitionedRDD.mapPartitions(iter => {
      val data = iter.toArray
      val index = Treap(data)
      Array(IPartition(data.map(_._2), index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    _indexedRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    TreapIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD)
      .asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    TreapIndexedRelation(new_output, child, table_name,
      column_keys, index_name)(_indexedRDD, range_bounds)
  }
}
