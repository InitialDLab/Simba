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

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.spatial._

import scala.collection.mutable

/**
  * Created by dong on 1/20/16.
  * Physical Scan on Indexed Relation
  */
private[sql] case class IndexedRelationScan(attributes: Seq[Attribute],
                                            predicates: Seq[Expression],
                                            relation: IndexedRelation)
  extends LeafNode with PredicateHelper {

  private val selectivity_enabled = sqlContext.conf.indexSelectivityEnable
  private val s_level_limit = sqlContext.conf.indexSelectivityLevel
  private val s_threshold = sqlContext.conf.indexSelectivityThreshold
  private val index_threshold = sqlContext.conf.indexSizeThreshold

  class DisOrdering(origin: Point, column_keys: List[Attribute]) extends Ordering[InternalRow] {
    def compare(a: InternalRow, b: InternalRow): Int = {
      var dis_a = 0.0
      var dis_b = 0.0
      for (i <- column_keys.indices) {
        val tmp_a = BindReferences.bindReference(column_keys(i), relation.output).eval(a)
          .asInstanceOf[Number].doubleValue()
        val tmp_b = BindReferences.bindReference(column_keys(i), relation.output).eval(b)
          .asInstanceOf[Number].doubleValue()
        dis_a += (tmp_a - origin.coord(i)) * (tmp_a - origin.coord(i))
        dis_b += (tmp_b - origin.coord(i)) * (tmp_b - origin.coord(i))
      }
      dis_a.compare(dis_b)
    }
  }

  // Tool function: Distance between row and point
  def evalDist(row: InternalRow, origin: Point, column_keys: List[Attribute]): Double = {
    var dis = 0.0
    for (i <- column_keys.indices) {
      val tmp = BindReferences.bindReference(column_keys(i), relation.output).eval(row)
        .asInstanceOf[Number].doubleValue()
      dis += (tmp - origin.coord(i)) * (tmp - origin.coord(i))
    }
    Math.sqrt(dis)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (predicates.size == 1 && predicates.head.toString == "true"){
      relation._indexedRDD.flatMap(_.data)
    } else relation match {
      case treemap @ TreeMapIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          val intervals = predicates.map(Interval.conditionToInterval(_, column_keys)._1)

          // global index
          val bounds = treemap.range_bounds
          val query_sets = intervals.map{
            in => Interval.getBoundNumberForInterval(in.headOption.orNull, bounds)
          }.reduce((a, b) => a ++ b).distinct
          val pruned = new PartitionPruningRDD(treemap._indexedRDD, query_sets.contains)

          val broadcastInterval = sparkContext.broadcast(intervals)
          // local index
          pruned flatMap {packed =>
            val index = packed.index.asInstanceOf[TreeMapIndex[Double]].index
            broadcastInterval.value.flatMap{interval_t =>
              // can be optimized if the local index bound is known
              val interval = interval_t.headOption.orNull
              var res = Array[Int]()
              if (interval != null && !interval.isNull) {
                res ++= index.subMap(interval.min._1, interval.max._1).values()
                  .toArray.map(_.asInstanceOf[Int])
                if (interval.max._2) res ++= Seq(index.get(interval.max._1)) // boundary
              }
              res.toIterable
            }.distinct.map(t => packed.data(t))
          }
        } else {
          treemap._indexedRDD.flatMap(_.data)
        }
      case treap @ TreapIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          val intervals = predicates.map(Interval.conditionToInterval(_, column_keys)._1)

          // global index
          val bounds = treap.range_bounds
          val query_sets = intervals.map{
            in => Interval.getBoundNumberForInterval(in.headOption.orNull, bounds)
          }.reduce((a, b) => a ++ b).distinct
          val pruned = new PartitionPruningRDD(treap._indexedRDD, query_sets.contains)

          val broadcastInterval = sparkContext.broadcast(intervals)
          // local index
          pruned.flatMap {packed => {
            val index = packed.index.asInstanceOf[Treap[Double]]
            broadcastInterval.value.flatMap{interval_t =>
              val interval = interval_t.headOption.orNull
              if (interval != null && !interval.isNull) {
                index.range(interval.min._1, interval.max._1).toIterable
              //  if (interval.max._2) tmp_res ++= index.find(interval.max._1)
              } else {
                Iterable.empty
              }
            }.distinct.map(t => packed.data(t))
          }}
        } else {
          treap._indexedRDD.flatMap(_.data)
        }
      case rtree @ RTreeIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          predicates.map { predicate =>
            val (intervals, exps) = Interval.conditionToInterval(predicate, column_keys)
            val queryMBR = new MBR(new Point(intervals.map(_.min._1)),
              new Point(intervals.map(_.max._1)))
            var cir_ranges = Array[(Point, Double)]()
            var knn_res: Array[InternalRow] = null

            exps.foreach {
              case InKNN(point: Seq[NamedExpression], target: Seq[Literal], l: Literal) =>
                val query_point = new Point(target.map(NumberConverter.literalToDouble).toArray)
                val ord = new DisOrdering(query_point, column_keys)
                val k = l.value.asInstanceOf[Number].intValue()

                def knnGlobalPrune(global_part: Seq[Int]): Array[InternalRow] = {
                  val pruned = new PartitionPruningRDD(rtree._indexedRDD, global_part.contains)
                  pruned.flatMap{ packed =>
                    var tmp_ans = Array[(Shape, Int)]()
                    if (packed.index.asInstanceOf[RTree] != null) {
                      tmp_ans = packed.index.asInstanceOf[RTree]
                        .kNN(query_point, k, keepSame = false)
                    }
                    tmp_ans.map(x => packed.data(x._2))
                  }.takeOrdered(k)(ord)
                }

                // first prune, get k partitions, but partitions may not be final partitions
                val global_part1 = rtree.global_rtree.kNN(query_point, {(a: Point, b: MBR) => b.maxDist(a)},
                  k, keepSame = false).map(_._2).toSeq
                val tmp_ans = knnGlobalPrune(global_part1) // to get a safe and tighter bound
                val theta = evalDist(tmp_ans.last, query_point, column_keys)

                // second prune, with the safe bound theta, to get the final global result
                val global_part2 = (rtree.global_rtree.circleRange(query_point, theta).map(_._2) diff global_part1).toSeq
                val tmp_knn_res = if (global_part2.isEmpty) tmp_ans
                else knnGlobalPrune(global_part2).union(tmp_ans).sorted(ord).take(k)

                if (knn_res == null) knn_res = tmp_knn_res
                else knn_res = knn_res.intersect(tmp_knn_res) // for multiple knn predicate
              case InCircleRange(point: Seq[NamedExpression], target: Seq[Literal], l: Literal) =>
                val query_point = new Point(target.map(NumberConverter.literalToDouble).toArray)
                val r = NumberConverter.literalToDouble(l)
                cir_ranges = cir_ranges :+ (query_point, r)
            }

            if (knn_res == null || knn_res.length > index_threshold) { // too large
              var global_part = rtree.global_rtree.range(queryMBR).map(_._2).toSeq
              if (cir_ranges.nonEmpty){ // circle range
                global_part = global_part.intersect(
                  rtree.global_rtree.circleRangeConj(cir_ranges).map(_._2)
                )
              }

              val pruned = new PartitionPruningRDD(rtree._indexedRDD, global_part.contains)

              val tmp_rdd = pruned.flatMap {packed =>
                val index = packed.index.asInstanceOf[RTree]
                if (index != null) {
                  val root_mbr = index.root.m_mbr
                  val perfect_cover = queryMBR.contains(root_mbr.low) &&
                    queryMBR.contains(root_mbr.high) &&
                    cir_ranges.forall(x => Dist.furthest(x._1, root_mbr) <= x._2)

                  if (perfect_cover) packed.data
                  else {
                    val range_res = if (selectivity_enabled) {
                      val res = index.range(queryMBR, s_level_limit, s_threshold)
                      if (res.isEmpty) { // full scan
                        packed.data.filter(row => queryMBR.intersects(new Point(
                          column_keys.map(x => BindReferences.bindReference(x, relation.output)
                            .eval(row).asInstanceOf[Number].doubleValue()).toArray
                        )))
                      } else res.get.map(x => packed.data(x._2))
                    } else index.range(queryMBR).map(x => packed.data(x._2))
                    if (cir_ranges.nonEmpty) { // circle range processing
                      range_res.intersect(index.circleRangeConj(cir_ranges)
                        .map(x => packed.data(x._2)))
                    }
                    else range_res
                  }
                } else Array[InternalRow]()
              }

              if (knn_res != null) sparkContext.parallelize(knn_res, 1).intersection(tmp_rdd)
              else tmp_rdd
            } else {
              val final_res = knn_res.filter {row =>
                val tmp_point = new Point(
                  column_keys.map(x => BindReferences.bindReference(x, relation.output)
                    .eval(row).asInstanceOf[Number].doubleValue()).toArray
                )

                val contain = cir_ranges.forall(x => tmp_point.minDist(x._1) <= x._2)
                contain && queryMBR.contains(tmp_point)
              }
              sparkContext.parallelize(final_res, 1)
            }
          }.reduce((a, b) => a.union(b)).map(_.copy()).distinct()
        } else rtree._indexedRDD.flatMap(_.data)
      case qtree @ QuadTreeIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty){
          predicates.map{ predicate =>
            val (intervals, exps) = Interval.conditionToInterval(predicate, column_keys)
            val queryMBR = new MBR(new Point(intervals.map(_.min._1)),
              new Point(intervals.map(_.max._1)))
            val global_part = qtree.global_index.range(queryMBR, searchMBR = true).map(_._2).toSeq
            val pruned = new PartitionPruningRDD(qtree._indexedRDD, global_part.contains)

            pruned.flatMap{packed =>
              val index = packed.index.asInstanceOf[QuadTree]
              if (index != null) index.range(queryMBR).map(x => packed.data(x._2)).iterator
              else Iterator[InternalRow]()
            }
          }.reduce(_ union _).map(_.copy()).distinct()
        } else qtree._indexedRDD.flatMap(_.data)
      case other =>
        other.indexedRDD.flatMap(_.data)
    }
  }

  override def output: Seq[Attribute] = relation.output
}