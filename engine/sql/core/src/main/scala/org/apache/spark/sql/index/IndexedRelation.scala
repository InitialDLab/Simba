package org.apache.spark.sql.index

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.spatial.{RTree, Point}
import org.apache.spark.sql.types.{IntegerType, DoubleType}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{Statistics, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.partitioner.{strRangePartition, RangePartition}
import org.apache.spark.storage.StorageLevel

/**
 * Created by crystalove on 15-5-27.
 */
case class PackedPartitionWithIndex(data : Array[Row], index : Index)

private[sql] object IndexedRelation{
  def apply(child: SparkPlan,
            tableName: Option[String],
            indexType: IndexType,
            columnKeys: List[Attribute],
            indexName: String) : IndexedRelation = {
    indexType match {
      case TreeMapType => new TreeMapIndexRelation(child.output, child, tableName, columnKeys, indexName)()
      case RTreeType => new RTreeIndexRelation(child.output, child, tableName, columnKeys, indexName)()
      case _ => null
    }
  }
}

private[sql] abstract class IndexedRelation extends LogicalPlan {
  self: Product =>
  var _indexedRDD: RDD[PackedPartitionWithIndex]
  def indexedRDD = _indexedRDD
  def sqlContext = SparkPlan.currentContext.get()
  def numShufflePartitions = sqlContext.conf.numShufflePartitions
  def maxEntriesPerNode = sqlContext.conf.maxEntriesPerNode
  def sampleRate = sqlContext.conf.sampleRate

  override def children = Seq.empty
  def output: Seq[Attribute]

  def withOutput(newOutput: Seq[Attribute]): IndexedRelation

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(sqlContext.conf.defaultSizeInBytes)
  )
}

private[sql] case class TreeMapIndexRelation(
    output: Seq[Attribute],
    child: SparkPlan,
    tableName: Option[String],
    columnKeys: List[Attribute],
    indexName: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null,
                       var range_bounds: Array[Double] = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(columnKeys.length == 1)

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[sql] def buildIndex() : Unit = {
    val dataRDD = child.execute().map(row => {
      val key = BindReferences
        .bindReference(columnKeys.head, child.output)
        .eval(row)
        .asInstanceOf[Number].doubleValue
      (key, row)
    })
    val (partitionedRDD, tmp_bounds) = RangePartition.rowPartition(dataRDD, numShufflePartitions)
    //partitionedRDD.collect().foreach(println)
    range_bounds = tmp_bounds
    val indexed = partitionedRDD.mapPartitions(iter => {
      val data = iter.toArray
      val index = TreeMapIndex(data)
      Array(PackedPartitionWithIndex(data.map(_._2), index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    //indexed.setName(tableName.map(n => s"TreeMap Indexed table $n").getOrElse(child.toString))
    indexed.setName(tableName.map(n => s"$n $indexName").getOrElse(child.toString))
    _indexedRDD = indexed
  }

  override def newInstance() = {
    new TreeMapIndexRelation(output.map(_.newInstance()), child, tableName, columnKeys, indexName)(_indexedRDD).asInstanceOf[this.type]
  }

  override def withOutput(newOutput: Seq[Attribute]): IndexedRelation = {
    TreeMapIndexRelation(newOutput, child, tableName, columnKeys, indexName)(_indexedRDD, range_bounds)
  }
}


private[sql] case class RTreeIndexRelation(
    output: Seq[Attribute],
    child: SparkPlan,
    tableName: Option[String],
    columnKeys: List[Attribute],
    indexName: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null,
                       var global_rtree: RTree = null)
  extends IndexedRelation with MultiInstanceRelation {

  require((columnKeys.head.dataType.isInstanceOf[DoubleType] || columnKeys.head.dataType.isInstanceOf[IntegerType])
          && (columnKeys.last.dataType.isInstanceOf[DoubleType] || columnKeys.head.dataType.isInstanceOf[IntegerType]))

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[sql] def buildIndex() : Unit = {
    val dataRDD = child.execute().map(row => {
      val now = columnKeys.map(x => BindReferences.bindReference(x, child.output).eval(row).asInstanceOf[Number].doubleValue).toArray
      (new Point(now), row)
    })

    val (partitionedRDD, mbr_bounds) = strRangePartition(dataRDD, columnKeys.length, numShufflePartitions, check_bound = true, sampleRate)

    val max_entries_per_node = maxEntriesPerNode
    val indexed = partitionedRDD.mapPartitions(iter => {
      val data = iter.toArray
      var index : RTree = null
      if (data.length > 0) index = RTree(data.map(_._1).zipWithIndex, data(0)._1.dimensions, max_entries_per_node)
      Array(PackedPartitionWithIndex(data.map(_._2), index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val partitionedSize = indexed.mapPartitions(iter => iter.map(_.data.length)).collect()

    global_rtree = RTree.buildFromMBR(mbr_bounds.zip(partitionedSize).map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)
    indexed.setName(tableName.map(n => s"$n $indexName").getOrElse(child.toString))
    _indexedRDD = indexed
  }

  override def newInstance() = {
    new RTreeIndexRelation(output.map(_.newInstance()), child, tableName, columnKeys, indexName)(_indexedRDD).asInstanceOf[this.type]
  }

  override def withOutput(newOutput: Seq[Attribute]): IndexedRelation = {
    RTreeIndexRelation(newOutput, child, tableName, columnKeys, indexName)(_indexedRDD, global_rtree)
  }
}
