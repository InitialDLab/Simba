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

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by dong on 1/15/16.
  * Indexed Relation Structures for Simba
  */

private[simba] case class IPartition(data: Array[InternalRow], index: Index)

private[simba] object IndexedRelation {
  def apply(child: SparkPlan, table_name: Option[String], index_type: IndexType,
            column_keys: List[Attribute], index_name: String): IndexedRelation = {
    index_type match {
      case TreeMapType =>
        TreeMapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case TreapType =>
        TreapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case RTreeType =>
        RTreeIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case HashMapType =>
        HashMapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case _ => null
    }
  }
}

private[simba] abstract class IndexedRelation extends LogicalPlan {
  self: Product =>
  var _indexedRDD: IndexedRDD
  def indexedRDD: IndexedRDD = _indexedRDD

  def simbaSession = SimbaSession.getActiveSession.orNull

  override def children: Seq[LogicalPlan] = Nil
  def output: Seq[Attribute]

  def withOutput(newOutput: Seq[Attribute]): IndexedRelation
}
