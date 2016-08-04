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

package org.apache.spark.sql.index

import org.apache.spark.sql.IndexRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.partitioner._
import org.apache.spark.sql.types.{DoubleType, IntegerType, NumericType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.util.FetchPointUtils

/**
 * Created by dong on 1/15/16.
 * Indexed Relation Structures for Simba
 */

private[sql] case class IPartition(data: Array[InternalRow], index: Index)

private[sql] object IndexedRelation {
  def apply(child: SparkPlan, table_name: Option[String], index_type: IndexType,
            column_keys: List[Attribute], index_name: String): IndexedRelation = {
    index_type match {
      case TreeMapType =>
        new TreeMapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case TreapType =>
        new TreapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case RTreeType =>
        new RTreeIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case HashMapType =>
        new HashMapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case QuadTreeType =>
        new QuadTreeIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case _ => null
    }
  }
}

private[sql] abstract class IndexedRelation extends LogicalPlan {
  self: Product =>
  var _indexedRDD: IndexRDD
  def indexedRDD: IndexRDD = _indexedRDD

  override def children: Seq[LogicalPlan] = Nil
  def output: Seq[Attribute]

  def withOutput(newOutput: Seq[Attribute]): IndexedRelation
}
