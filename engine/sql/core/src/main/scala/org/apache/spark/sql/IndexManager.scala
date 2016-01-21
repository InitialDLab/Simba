/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.index._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by dong on 1/20/16.
  * Index Manager for Simba
  */
private case class IndexedData(name: String, plan: LogicalPlan, indexedData: IndexedRelation)

case class IndexInfo(tableName: String, indexName: String,
                     attributes: Seq[Attribute], indexType: IndexType,
                     location: String, storageLevel: StorageLevel) extends Serializable

private[sql] class IndexManager extends Logging {
  @transient
  private val indexedData = new ArrayBuffer[IndexedData]

  @transient
  private val indexLock = new ReentrantReadWriteLock

  @transient
  private val indexInfos = new ArrayBuffer[IndexInfo]

  def getIndexInfo: Array[IndexInfo] = indexInfos.toArray

  private def readLock[A](f: => A): A = {
    val lock = indexLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  private def writeLock[A](f: => A): A = {
    val lock = indexLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  private[sql] def isEmpty: Boolean = readLock {
    indexedData.isEmpty
  }

  private[sql] def lookupIndexedData(query: DataFrame): Option[IndexedData] = readLock {
    indexedData.find(cd => query.queryExecution.analyzed.sameResult(cd.plan))
  }

  private[sql] def lookupIndexedData(plan: LogicalPlan): Option[IndexedData] = readLock {
    indexedData.find(cd => plan.sameResult(cd.plan))
  }

  private[sql] def lookupIndexedData(query: DataFrame, indexName: String): Option[IndexedData] =
    readLock {
      lookupIndexedData(query.queryExecution.analyzed, indexName)
    }

  private[sql] def lookupIndexedData(plan: LogicalPlan, indexName: String): Option[IndexedData] =
    readLock {
      indexedData.find(cd => plan.sameResult(cd.plan) && cd.name.equals(indexName))
    }


  private[sql] def persistIndex(sqlContext: SQLContext,
                                indexName: String,
                                fileName: String): Unit = {
    val dataIndex = indexedData.indexWhere(cd => cd.name.equals(indexName))
    require(dataIndex >= 0, "Index not found!")
    val preData = indexInfos(dataIndex)
    val indexedItem = indexedData(dataIndex)

    sqlContext.sparkContext.parallelize(Array(indexedItem.plan))
      .saveAsObjectFile(fileName + "/plan")
    sqlContext.sparkContext.parallelize(Array(preData)).saveAsObjectFile(fileName + "/indexInfo")
    if (preData.indexType == RTreeType) {
      val rtreeRelation = indexedItem.indexedData.asInstanceOf[RTreeIndexedRelation]
      sqlContext.sparkContext.parallelize(Array(rtreeRelation))
        .saveAsObjectFile(fileName + "/rtreeRelation")
      rtreeRelation._indexedRDD.saveAsObjectFile(fileName + "/rdd")
    } else {
      val treeMapRelation = indexedItem.indexedData.asInstanceOf[TreeMapIndexedRelation]
      sqlContext.sparkContext.parallelize(Array(treeMapRelation))
        .saveAsObjectFile(fileName + "/treeMapRelation")
      treeMapRelation._indexedRDD.saveAsObjectFile(fileName + "/rdd")
    }

    indexInfos(dataIndex) = IndexInfo(preData.tableName, preData.indexName,
      preData.attributes, preData.indexType, fileName, preData.storageLevel)
  }

  private[sql] def loadIndex(sqlContext: SQLContext,
                             indexName: String,
                             fileName: String): Unit = {
    val info = sqlContext.sparkContext.objectFile[IndexInfo](fileName + "/indexInfo").collect().head
    val plan = sqlContext.sparkContext.objectFile[LogicalPlan](fileName + "/plan").collect().head
    val rdd = sqlContext.sparkContext.objectFile[PackedPartitionWithIndex](fileName + "/rdd")
    if (info.indexType == RTreeType){
      val rtreeRelation = sqlContext.sparkContext
        .objectFile[RTreeIndexedRelation](fileName + "/rtreeRelation").collect().head
      indexedData += IndexedData(indexName, plan,
        RTreeIndexedRelation(rtreeRelation.output, rtreeRelation.child,
          rtreeRelation.table_name, rtreeRelation.column_keys,
          rtreeRelation.index_name)(rdd, rtreeRelation.global_rtree))
    } else {
      val treeMapRelation = sqlContext.sparkContext
        .objectFile[TreeMapIndexedRelation](fileName + "/treeMapRelation").collect().head
      indexedData += IndexedData(indexName, plan,
        TreeMapIndexedRelation(treeMapRelation.output, treeMapRelation.child,
          treeMapRelation.table_name, treeMapRelation.column_keys,
          treeMapRelation.index_name)(rdd, treeMapRelation.range_bounds))
    }
    indexInfos += info
  }


  private[sql] def setStorageLevel(query: DataFrame,
                                   indexName: String,
                                   newLevel: StorageLevel): Unit = writeLock {
    val dataIndex = indexedData.indexWhere {
      cd => query.queryExecution.analyzed.sameResult(cd.plan) && cd.name.equals(indexName)
    }
    require(dataIndex >= 0, "Index not found!")
    val preData = indexInfos(dataIndex)
    indexInfos(dataIndex) = IndexInfo(preData.tableName, preData.indexName, preData.attributes,
                              preData.indexType, preData.location, newLevel)
  }

  private[sql] def createIndexQuery(query: DataFrame,
                                    indexType: IndexType,
                                    indexName: String,
                                    column: List[Attribute],
                                    tableName: Option[String] = None,
                                    storageLevel: StorageLevel = MEMORY_AND_DISK): Unit =
    writeLock {
      val planToIndex = query.queryExecution.analyzed
      if (lookupIndexedData(planToIndex).nonEmpty) {
        // scalastyle:off println
        println("Index for the data has already been built.")
        // scalastyle:on println
      } else {
        val sqlContext = query.sqlContext
        indexedData +=
          IndexedData(indexName, planToIndex,
            IndexedRelation(sqlContext.executePlan(planToIndex).executedPlan, tableName,
              indexType, column, indexName))
        indexInfos += IndexInfo(tableName.getOrElse("anonymous"), indexName,
          column, indexType, "", storageLevel)
      }
    }

  private[sql] def showQuery(sqlContext: SQLContext, tableName: String): Unit = readLock {
    val query = sqlContext.table(tableName)
    indexInfos.map(row => {
      if (row.tableName.equals(tableName)) {
        // scalastyle:off println
        println("Index " + row.indexName + " {")
        println("\tTable: " + tableName)
        print("\tOn column: (")
        for (i <- row.attributes.indices)
          if (i != row.attributes.length - 1) {
            print(row.attributes(i).name + ", ")
          } else println(row.attributes(i).name + ")")
        println("\tIndex Type: " + row.indexType.toString)
        println("}")
        // scalastyle:on println
      }
      row
    })
  }

  private[sql] def dropIndexQuery(query: DataFrame, blocking: Boolean = true): Unit = writeLock {
    val planToIndex = query.queryExecution.analyzed
    var hasFound = false
    var found = true
    while (found) {
      val dataIndex = indexedData.indexWhere(cd => planToIndex.sameResult(cd.plan))
      if (dataIndex < 0) found = false
      else hasFound = true
      indexedData(dataIndex).indexedData.indexedRDD.unpersist(blocking)
      indexedData.remove(dataIndex)
      indexInfos.remove(dataIndex)
    }
    indexedData
  }

  private[sql] def dropIndexByNameQuery(query: DataFrame,
                                        indexName: String,
                                        blocking: Boolean = true): Unit = writeLock {
    val planToIndex = query.queryExecution.analyzed
    val dataIndex = indexedData.indexWhere { cd =>
      planToIndex.sameResult(cd.plan) && cd.name.equals(indexName)
    }
    require(dataIndex >= 0, s"Table $query or index $indexName is not indexed.")
    indexedData(dataIndex).indexedData.indexedRDD.unpersist(blocking)
    indexedData.remove(dataIndex)
    indexInfos.remove(dataIndex)
  }

  private[sql] def dropIndexByColumnQuery(query: DataFrame,
                                          column: List[Attribute],
                                          blocking: Boolean = true) : Unit = writeLock {
    val planToIndex = query.queryExecution.analyzed
    var dataIndex = -1
    for (i <- 0 to indexInfos.length) {
      val cd = indexedData(i)
      val row = indexInfos(i)
      if (planToIndex.sameResult(cd.plan) && row.attributes.equals(column)) {
        dataIndex = i
      }
    }
    require(dataIndex >= 0, s"Table $query or Index on $column is not indexed.")
    indexedData(dataIndex).indexedData.indexedRDD.unpersist(blocking)
    indexedData.remove(dataIndex)
    indexInfos.remove(dataIndex)
  }

  private[sql] def tryDropIndexQuery(query: DataFrame,
                                     blocking: Boolean = true): Boolean = writeLock {
    val planToIndex = query.queryExecution.analyzed
    var found = true
    var hasFound = false
    while (found) {
      val dataIndex = indexedData.indexWhere(cd => planToIndex.sameResult(cd.plan))
      found = dataIndex >= 0
      if (found) {
        hasFound = true
        indexedData(dataIndex).indexedData.indexedRDD.unpersist(blocking)
        indexedData.remove(dataIndex)
        indexInfos.remove(dataIndex)
      }
    }
    hasFound
  }

  private[sql] def tryDropIndexByNameQuery(query: DataFrame,
                                           indexName: String,
                                           blocking: Boolean = true): Boolean = writeLock {
    val planToCache = query.queryExecution.analyzed
    val dataIndex = indexedData.indexWhere(cd => planToCache.sameResult(cd.plan))
    val found = dataIndex >= 0
    if (found) {
      indexedData(dataIndex).indexedData.indexedRDD.unpersist(blocking)
      indexedData.remove(dataIndex)
      indexInfos.remove(dataIndex)
    }
    found
  }

  private[sql] def clearIndex(): Unit = writeLock {
    indexedData.foreach(_.indexedData.indexedRDD.unpersist())
    indexedData.clear()
    indexInfos.clear()
  }

  private[sql] def useIndexedData(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      case currentFragment =>
        lookupIndexedData(currentFragment)
          .map(_.indexedData.withOutput(currentFragment.output))
          .getOrElse(currentFragment)
    }
  }
}
