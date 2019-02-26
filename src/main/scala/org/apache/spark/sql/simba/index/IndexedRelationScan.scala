
package org.apache.spark.sql.simba.index

import org.apache.spark.sql.simba.execution.SimbaPlan
import org.apache.spark.sql.simba.expression.{InCircleRange, InKNN}
import org.apache.spark.sql.simba.spatial.{Dist, MBR, Point, Shape}
import org.apache.spark.sql.simba.util.{NumberUtil, ShapeUtils}
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by gefei on 2016/11/14.
  */
private[simba] case class IndexedRelationScan(attributes: Seq[Attribute],
                                              predicates: Seq[Expression],
                                              relation: IndexedRelation)
  extends SimbaPlan with PredicateHelper {

  private val selectivity_enabled = simbaSessionState.simbaConf.indexSelectivityEnable
  private val s_level_limit = simbaSessionState.simbaConf.indexSelectivityLevel
  private val s_threshold = simbaSessionState.simbaConf.indexSelectivityThreshold
  private val index_threshold = simbaSessionState.simbaConf.indexSizeThreshold

  override def children:Seq[SparkPlan] = Nil // for UnaryNode

  class DisOrdering(origin: Point, column_keys: List[Attribute], isPoint: Boolean)
    extends Ordering[InternalRow] {
    def compare(a: InternalRow, b: InternalRow): Int = {
      val a_point = ShapeUtils.getPointFromRow(a, column_keys, relation, isPoint)
      val b_point = ShapeUtils.getPointFromRow(b, column_keys, relation, isPoint)
      origin.minDist(a_point).compare(origin.minDist(b_point))
    }
  }

  // Tool function: Distance between row and point
  def evalDist(row: InternalRow, origin: Point, column_keys: List[Attribute],
               isPoint: Boolean): Double = {
    origin.minDist(ShapeUtils.getPointFromRow(row, column_keys, relation, isPoint))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val after_filter = if (predicates.size == 1 && predicates.head.toString == "true"){
      relation._indexedRDD.flatMap(_.data)
    } else relation match {
      case treemap @ TreeMapIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          // for treemap, the length of column_keys is 1
          val intervals = predicates.map(Interval.conditionToInterval(_, column_keys, 1)._1)

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
          val intervals = predicates.map(Interval.conditionToInterval(_, column_keys, 1)._1)

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
          val res_rdds = predicates.map { predicate =>
            val (intervals, exps, flag) = Interval.conditionToInterval(predicate, column_keys,
              rtree.dimension)
            val queryMBR = new MBR(new Point(intervals.map(_.min._1)),
              new Point(intervals.map(_.max._1)))
            var cir_ranges = Array[(Point, Double)]()
            var knn_res: Array[InternalRow] = null

            exps.foreach {
              case InKNN(point: Expression, target: Literal, l: Literal) =>
                val query_point = target.value.asInstanceOf[Point]
                val ord = new DisOrdering(query_point, column_keys, rtree.isPoint)
                val k = l.value.asInstanceOf[Number].intValue()

                def knnGlobalPrune(global_part: Set[Int]): Array[InternalRow] = {
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
                val global_part1 = rtree.global_rtree.kNN(query_point, k, keepSame = false).map(_._2).toSet
                val tmp_ans = knnGlobalPrune(global_part1) // to get a safe and tighter bound
                val theta = evalDist(tmp_ans.last, query_point, column_keys, rtree.isPoint)

                // second prune, with the safe bound theta, to get the final global result
                val global_part2 = rtree.global_rtree.circleRange(query_point, theta).
                  map(_._2).toSet -- global_part1
                val tmp_knn_res = if (global_part2.isEmpty) tmp_ans
                else knnGlobalPrune(global_part2).union(tmp_ans).sorted(ord).take(k)

                if (knn_res == null) knn_res = tmp_knn_res
                else knn_res = knn_res.intersect(tmp_knn_res)
              case InCircleRange(point: Expression, target: Literal, l: Literal) =>
                val query_point = target.value.asInstanceOf[Point]
                val r = NumberUtil.literalToDouble(l)
                cir_ranges = cir_ranges :+ (query_point, r)
            }

            if (knn_res == null || (!flag && knn_res.length > index_threshold)) { // too large
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
                        packed.data.filter(row => queryMBR.intersects(
                          ShapeUtils.getPointFromRow(row, column_keys, relation, rtree.isPoint)
                        ))
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
                val tmp_point = ShapeUtils.getPointFromRow(row, column_keys, relation,
                  rtree.isPoint)
                val contain = cir_ranges.forall(x => tmp_point.minDist(x._1) <= x._2)
                contain && queryMBR.contains(tmp_point)
              }
              sparkContext.parallelize(final_res, 1)
            }
          }
          if (predicates.length > 1) res_rdds.reduce((a, b) => a.union(b)).map(_.copy()).distinct()
          else res_rdds.head
        } else rtree._indexedRDD.flatMap(_.data)
      case qtree @ QuadTreeIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty){
          predicates.map{ predicate =>
            val (intervals, exps, flag) = Interval.conditionToInterval(predicate, column_keys, 2)
            val queryMBR = MBR(Point(intervals.map(_.min._1)),
              Point(intervals.map(_.max._1)))
            var global_part = qtree.global_index.range(queryMBR, searchMBR = true).map(_._2).toSeq
            var circle_ranges = Array[(Point, Double)]()
            exps.foreach { // InCircleRange global pruning
              case InCircleRange(point: Expression, target: Literal, l: Literal) =>
                val query_point = target.value.asInstanceOf[Point]
                val r = NumberUtil.literalToDouble(l)
                circle_ranges = circle_ranges :+ (query_point -> r)
                global_part = global_part.intersect(
                  qtree.global_index.circleRange(query_point, r, searchMBR = true))
            }

            val pruned = new PartitionPruningRDD(qtree._indexedRDD, global_part.contains)
            pruned.flatMap{packed =>
              val index = packed.index.asInstanceOf[QuadTree]
              if (index != null){
                val range_res = index.range(queryMBR)
                val temp = if (circle_ranges.nonEmpty) {
                  val circle_res = circle_ranges.map{cir =>
                    index.circleRange(cir._1, cir._2)
                  }.reduce((a, b) => a.union(b))
                  range_res.intersect(circle_res)
                } else range_res
                temp.map(x => packed.data(x._2)).iterator
              } else Iterator[InternalRow]()
            }
          }.reduce(_ union _).map(_.copy()).distinct()
        } else qtree._indexedRDD.flatMap(_.data)
      case other =>
        other.indexedRDD.flatMap(_.data)
    }


    after_filter.mapPartitions { // performance track
      iter =>
        val project = UnsafeProjection.create(attributes, relation.output,
          subexpressionEliminationEnabled = true)
        iter.map { row => project(row) }
    }
  }

  override def output: Seq[Attribute] = attributes
}