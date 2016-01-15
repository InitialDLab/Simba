package org.apache.spark.sql

import java.util.concurrent.locks.ReentrantReadWriteLock
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.index.{IndexType, IndexedRelation}
import scala.collection.mutable.ArrayBuffer

/**
 * Created by crystalove on 15-5-27.
 */
private case class IndexedData(name: String, plan: LogicalPlan, indexedData: IndexedRelation)
case class IndexInfo(tableName: String, indexName:String, attributes: Seq[Attribute], indexType: IndexType, location: String, storageLevel: StorageLevel)

private[sql] class IndexManager(sqlContext: SQLContext) extends Logging {
    @transient
    private val indexedData = new scala.collection.mutable.ArrayBuffer[IndexedData]

    @transient
    private val indexLock = new ReentrantReadWriteLock

    @transient
    private val indexInfos = new ArrayBuffer[IndexInfo]

    def hasIndex(tableName: String, indexName : String): Boolean = lookupIndexedData(sqlContext.table(tableName), indexName).nonEmpty

    def showIndex(tableName: String): Unit = showQuery(tableName)

    def createIndex(tableName : String, indexType : IndexType, indexName: String, column: List[Attribute]) : Unit = createIndexQuery(sqlContext.table(tableName), indexType, indexName, column, Some(tableName))

    def dropIndexByName(tableName: String, indexName : String): Unit = dropIndexByNameQuery(sqlContext.table(tableName), indexName)

    def getIndexInfo = indexInfos

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

    private[sql] def lookupIndexedData(query: DataFrame, indexName: String): Option[IndexedData] = readLock {
        lookupIndexedData(query.queryExecution.analyzed, indexName)
    }

    private[sql] def lookupIndexedData(plan: LogicalPlan, indexName: String): Option[IndexedData] = readLock {
        indexedData.find(cd => plan.sameResult(cd.plan) && cd.name.equals(indexName))
    }

    private[sql] def persistIndex(indexName: String, fileName: String): Unit = {
        val dataIndex = indexedData.indexWhere(cd => cd.name.equals(indexName))
        assert(dataIndex >= 0, "Index not found!")
        val preData = indexInfos(dataIndex)
        indexedData(dataIndex).indexedData._indexedRDD.saveAsObjectFile(fileName)
        indexInfos(dataIndex) = IndexInfo(preData.tableName, preData.indexName, preData.attributes, preData.indexType, fileName, preData.storageLevel)
    }

    private[sql] def loadIndex(indexName: String, fileName: String): Unit = {
        //TODO implement load index
    }

    private[sql] def setStorageLevel(
        query: DataFrame,
        indexName: String,
        newLevel: StorageLevel): Unit = writeLock {
        val dataIndex = indexedData.indexWhere(cd => query.queryExecution.analyzed.sameResult(cd.plan) && cd.name.equals(indexName))
        assert(dataIndex >= 0, "Index not found!")
        val preData = indexInfos(dataIndex)
        indexInfos(dataIndex) = IndexInfo(preData.tableName, preData.indexName, preData.attributes, preData.indexType, preData.location, newLevel)
    }

    private[sql] def createIndexQuery(
        query: DataFrame,
        indexType: IndexType,
        indexName: String,
        column: List[Attribute],
        tableName: Option[String] = None,
        storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = writeLock {
        val planToIndex = query.queryExecution.analyzed
        if (lookupIndexedData(planToIndex).nonEmpty) {
            println("Index for the data has already been built.")
        } else {
            indexedData +=
            IndexedData(indexName, planToIndex, IndexedRelation(query.queryExecution.executedPlan, tableName, indexType, column, indexName))
            indexInfos += IndexInfo(tableName.getOrElse("anonymous"), indexName, column, indexType, "", storageLevel)
        }
    }

    private[sql] def showQuery(tableName: String): Unit = readLock {
        val query = sqlContext.table(tableName)
        val planToShow = query.queryExecution.analyzed
        indexInfos.map(row => {
            if (row.tableName.equals(tableName)) {
                println("Index " + row.indexName + " {")
                println("\tTable: " + tableName)
                print("\tOn column: (")
                for (i <- 0 to row.attributes.length - 1)
                    if (i != row.attributes.length - 1)
                        print(row.attributes(i).name + ", ")
                    else println(row.attributes(i).name + ")")
                println("\tIndex Type: " + row.indexType.toString)
                println("}")
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

    private[sql] def dropIndexByNameQuery(query: DataFrame, indexName: String, blocking: Boolean = true): Unit = writeLock {
        val planToIndex = query.queryExecution.analyzed
        val dataIndex = indexedData.indexWhere(cd => planToIndex.sameResult(cd.plan) && cd.name.equals(indexName))
        require(dataIndex >= 0, s"Table $query or index $indexName is not indexed.")
        indexedData(dataIndex).indexedData.indexedRDD.unpersist(blocking)
        indexedData.remove(dataIndex)
        indexInfos.remove(dataIndex)
    }

    private[sql] def dropIndexByColumnQuery(query: DataFrame, column: List[Attribute], blocking: Boolean = true): Unit = writeLock {
        val planToIndex = query.queryExecution.analyzed
        var dataIndex = -1
        for (i <- 0 to indexInfos.length) {
            val cd = indexedData(i)
            val row = indexInfos(i)
            if (planToIndex.sameResult(cd.plan) && row.attributes.equals(column))
                dataIndex = i
        }
        require(dataIndex >= 0, s"Table $query or Index on $column is not indexed.")
        indexedData(dataIndex).indexedData.indexedRDD.unpersist(blocking)
        indexedData.remove(dataIndex)
        indexInfos.remove(dataIndex)
    }

    private[sql] def tryDropIndexQuery(
        query: DataFrame,
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

    private[sql] def tryDropIndexByNameQuery(
            query: DataFrame,
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
            case currentFragment => {
                lookupIndexedData(currentFragment)
                  .map(_.indexedData.withOutput(currentFragment.output))
                  .getOrElse(currentFragment)
            }
        }
    }
}
