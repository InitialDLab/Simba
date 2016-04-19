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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.spatial.{Dist, MBR, Point, RTree}

import scala.collection.mutable

/**
 * Created by gefei on 15-5-29.
 * IndexRelationScan
 */
private[sql] case class IndexRelationScan(
   attributes: Seq[Attribute],
   predicates: Seq[Expression],
   relation: IndexedRelation)
  extends LeafNode with PredicateHelper{
  def output: Seq[Attribute] = relation.output

  // end point (Double, Boolean): (end_value, isClosed)
  class Interval(var min: (Double, Boolean), var max:(Double, Boolean)) extends Serializable{
    def this(min_value: Double = 0.0, max_value: Double = 0.0, left_is_close: Boolean = true, right_is_close: Boolean = true){
      this((min_value,left_is_close), (max_value, right_is_close))
    }
    def intersect(interval: Interval): Interval = {
      val new_interval = new Interval()
      if (!interval.isNull){
        if (interval.min._1 > this.max._1 || interval.max._1 < this.min._1)
          new_interval.max = (new_interval.min._1 - 1, true) // null
        else {
          new_interval.min = if (this.min._1 < interval.min._1) interval.min else this.min
          new_interval.max = if (this.max._1 > interval.max._1) interval.max else this.max
        }
      }else {
        new_interval.min = this.min
        new_interval.max = this.max
      }
      new_interval
    }
    def isNull = min._1 > max._1 || (min._1 == max._1 && !(min._2 && max._2))
    override def toString =
      (if (min._2) "[" else "(") + min._1 + ", " + max._1 + (if (max._2) "]" else ")")
  }

  def getLeafInterval(leaf: Expression): (Interval, Attribute) = {
    leaf match {
      case LessThan(left: NamedExpression, right: Literal) =>
        (new Interval(Double.MinValue, right.value.asInstanceOf[Number].doubleValue, false, false), left.toAttribute)
      case EqualTo(left: NamedExpression, right: Literal) =>
        val temp = right.value.asInstanceOf[Number].doubleValue
        (new Interval(temp, temp), left.toAttribute)
      case LessThanOrEqual(left: NamedExpression, right: Literal) =>
        (new Interval(Double.MinValue, right.value.asInstanceOf[Number].doubleValue, false, true), left.toAttribute)
      case GreaterThan(left: NamedExpression, right: Literal) =>
        (new Interval(right.value.asInstanceOf[Number].doubleValue, Double.MaxValue, false, false), left.toAttribute)
      case GreaterThanOrEqual(left: NamedExpression, right: Literal) =>
        (new Interval(right.value.asInstanceOf[Number].doubleValue, Double.MaxValue, true, false), left.toAttribute)
      case _ =>
        null
    }
  }

  // returnType: interval_for_x, interval_for_y, expressions_can_not_be_convent_to_interval
  def conditionToInterval(expression: Expression, column: List[Attribute]): (Array[Interval], List[Expression]) = {
    val leafNodes = splitCNFPredicates(expression)
    val intervals : Array[Interval] = new Array[Interval](column.length)
    for (i <- 0 to column.length - 1)
      intervals(i) = new Interval(Double.MinValue, Double.MaxValue, false, false)
    var returnExpression: List[Expression] = List()
    leafNodes.foreach(leaf =>{
      val temp_interval = getLeafInterval(leaf)
      if (temp_interval != null){
        for (i <- 0 to column.length - 1)
          if (column.indexOf(temp_interval._2) == i)
            intervals(i) = intervals(i).intersect(temp_interval._1)
      } else {
        leaf match {
          case InRange(point: Seq[NamedExpression], boundL: Seq[Expression], boundR: Seq[Expression]) =>
            //TODO It is not a good hack to regard all bounds as Literal
            for (i <- 0 to point.length - 1)
              intervals(i) = intervals(i).intersect(new Interval(boundL(i).asInstanceOf[Literal].toString.toDouble, boundR(i).asInstanceOf[Literal].toString.toDouble))
          case knn @ InKNN(point: Seq[NamedExpression], target: Seq[Expression], k: Literal) =>
              returnExpression = returnExpression :+ knn
          case cr @ InCircleRange(point: Seq[NamedExpression], target: Seq[Expression], r: Literal) =>
              returnExpression = returnExpression :+ cr
          case _ =>
            //println("Error happen!!!")
        }
      }
    })
    (intervals, returnExpression)
  }

  class DisOrdering(origin: Point, columnKeys: List[Attribute]) extends Ordering[Row] {
    def compare(a : Row, b : Row) = {
      var distanceA : Double = 0
      val tmpa : Array[Double] = new Array[Double](columnKeys.length)
      for (i <-0 to columnKeys.length - 1) {
        tmpa(i) = BindReferences.bindReference(columnKeys(i), relation.output).eval(a).asInstanceOf[Number].doubleValue()
        distanceA += (tmpa(i) - origin.coord(i)) * (tmpa(i) - origin.coord(i))
      }
      var distanceB : Double = 0
      val tmpb : Array[Double] = new Array[Double](columnKeys.length)
      for (i <-0 to columnKeys.length - 1) {
        tmpb(i) = BindReferences.bindReference(columnKeys(i), relation.output).eval(b).asInstanceOf[Number].doubleValue()
        distanceB += (tmpb(i) - origin.coord(i)) * (tmpb(i) - origin.coord(i))
      }
      distanceA.compare(distanceB)
    }
  }

  def evalDist(row: Row, origin: Point, columnKeys: List[Attribute]) = {
    val tmpa : Array[Double] = new Array[Double](columnKeys.length)
    for (i <-0 to columnKeys.length - 1)
      tmpa(i) = BindReferences.bindReference(columnKeys(i), relation.output).eval(row).asInstanceOf[Number].doubleValue()
    origin.minDist(new Point(tmpa))
  }

  def execute(): RDD[Row] = {
    relation match {
      case treemap @ TreeMapIndexRelation(_, _, _, _, _) =>{
        if (predicates.length != 0){
          val intervals = predicates.map(conditionToInterval(_, treemap.columnKeys)._1).head
          val bounds = treemap.range_bounds
          val query_sets = new mutable.HashSet[Int]
          intervals.foreach(interval => if (interval != null && !interval.isNull){
            val start = bounds.indexWhere(ele => ele >= interval.min._1)
            var end = bounds.indexWhere(ele => ele >= interval.max._1)
            if (end == -1) end = bounds.length
            // make sure every interval can be added
            if (start >= 0)
              for (i <- start to end + 1)
                query_sets.add(i)
            else
              query_sets.add(bounds.length)
          })
          val pruned = new PartitionPruningRDD[PackedPartitionWithIndex](treemap._indexedRDD, query_sets.contains)
          pruned.flatMap(packed => {
            val index = packed.index.asInstanceOf[TreeMapIndex[Double]].index
            var temp_res: Array[Int] = Array()
            intervals.foreach(interval => {
              if (interval != null && !interval.isNull) {
                val temp = index.subMap(interval.min._1, interval.max._1).values().toArray.map(_.asInstanceOf[Int])
                temp_res = temp_res ++ temp
                if (interval.max._2) temp_res = temp_res ++ Array(index.get(interval.max._1))
              }
            })
            //array distinct do not need to invoke copy method.
            temp_res.distinct.map(t => packed.data(t)).iterator
          })
        } else
          treemap._indexedRDD.flatMap(_.data.iterator)
      }
      case rtree @ RTreeIndexRelation(_, _, _, _, _) =>{
        //println("length = " + predicates.length)
        if (predicates.length != 0)
          predicates.map(predicate => {
            //exps contains CircleRange and KNN
            val (intervals, exps) = conditionToInterval(predicate, rtree.columnKeys)
            var knn_result: Array[Row] = null
            val minPoint = new Array[Double](intervals.length)
            val maxPoint = new Array[Double](intervals.length)
            for (i <- 0 to intervals.length - 1) {
              minPoint(i) = intervals(i).min._1
              maxPoint(i) = intervals(i).max._1
            }
            val queryMBR = new MBR(new Point(minPoint), new Point(maxPoint))  // range
            var circle_query: Array[(Point, Double)] = Array()

            exps.foreach({
              case InKNN(point: Seq[NamedExpression], target: Seq[Literal], k: Literal) =>
                val pointArray = new Array[Double](point.length)
                for (i <- 0 to point.length - 1)
                  pointArray(i) = target(i).value.asInstanceOf[Number].doubleValue()
                val knn_point = new Point(pointArray)
                val knn_k = k.value.asInstanceOf[Number].intValue()
                val mbr_ans = rtree.global_rtree.kNNpMBR(knn_point, Dist.furthestpMBR, knn_k, keepSame = false)
                //println("length = " + mbr_ans.length)
                val ord = new DisOrdering(knn_point, rtree.columnKeys)
                val tmp_set = new mutable.HashSet[Int]()
                tmp_set ++= mbr_ans.map(_._2)
                val tmp_pruned = new PartitionPruningRDD[PackedPartitionWithIndex](rtree._indexedRDD, tmp_set.contains)
                val tmp_ans = tmp_pruned.flatMap(packed => {
                  var temp_ans = Array[(Point, Int)]()
                  if (packed.index.asInstanceOf[RTree] != null)
                    temp_ans = packed.index.asInstanceOf[RTree].kNN(knn_point, knn_k, keepSame =  false)
                  temp_ans.map(p => packed.data(p._2))
                }).takeOrdered(knn_k)(ord)
                val theta = evalDist(tmp_ans.last, knn_point, rtree.columnKeys)

                //println("theta = " + theta)
                val set = new mutable.HashSet[Int]
                set ++= rtree.global_rtree.circleRangeMBR(knn_point, theta).map(_._2)
                set --= tmp_set
                val temp_knn_result = if (set.isEmpty) tmp_ans
                else {
                  val pruned = new PartitionPruningRDD[PackedPartitionWithIndex](rtree._indexedRDD, set.contains)
                  pruned.flatMap(packed => {
                    var temp_ans = Array[(Point, Int)]()
                    if (packed.index.asInstanceOf[RTree] != null)
                      temp_ans = packed.index.asInstanceOf[RTree].kNN(knn_point, knn_k, keepSame =  false)
                    temp_ans.map(p => packed.data(p._2))
                  }).takeOrdered(knn_k)(ord).union(tmp_ans).sorted(ord).take(knn_k)
                }

                //val temp_knn_result = tmp_ans.union(tmp_ans_2).sorted(ord).take(knn_k)
                if (knn_result == null) knn_result = temp_knn_result
                else knn_result = knn_result.intersect(temp_knn_result)
              case InCircleRange(point: Seq[NamedExpression], target: Seq[Literal], r: Literal) =>
                val pointArray = new Array[Double](point.length)
                for (i <- 0 to point.length - 1)
                  pointArray(i) = target(i).value.asInstanceOf[Number].doubleValue()
                val query_point = new Point(pointArray)
                val query_radius = r.value.asInstanceOf[Number].doubleValue()
                circle_query = circle_query :+ (query_point, query_radius)
              case _ =>
            })

            if (knn_result == null || knn_result.length > sqlContext.conf.indexSizeThreshold){
              //global index
              var part = rtree.global_rtree.rangeMBR(queryMBR).map(_._2)  //range
              val circle_range = rtree.global_rtree.circleRangeMBRConjunctive(circle_query).map(_._2)
              part = part.intersect(circle_range)    //circle range
              val pruned = new PartitionPruningRDD[PackedPartitionWithIndex](rtree._indexedRDD, part.contains)
              // local index
              val temp_rdd = pruned.flatMap(packed => {
                val index = packed.index.asInstanceOf[RTree]

                if (index != null) {
                  // if queryMBR contains the partition bounds, then the whole partition should be mapped
                  val index_mbr = index.root.m_mbr
                  val range_result: Array[Row] = if (queryMBR.contains(index_mbr.low) && queryMBR.contains(index_mbr.high))
                    packed.data
                  else
                    index.range(queryMBR).map(_._2).map(packed.data(_))

                  val wholePartitionRemain = circle_query.map(item => Dist.furthestpMBR(item._1, index_mbr) <= item._2).reduceLeftOption(_ && _)
                  if (wholePartitionRemain.getOrElse(true)) // circle_query == null iff canBePruned == None
                    range_result
                  else{
                    val temp = index.circleRangeConjunctive(circle_query).map(_._2)
                    range_result.intersect(temp.map(packed.data(_)))
                  }
                }else
                  Array[Row]()
              })
              if (knn_result != null)
                sparkContext.parallelize(knn_result, 1).intersection(temp_rdd)
              else
                temp_rdd
            } else{
              val final_res = knn_result.filter(row => {
                val tmp : Array[Double] = new Array[Double](rtree.columnKeys.length)
                for (i <-0 to rtree.columnKeys.length - 1)
                  tmp(i) = BindReferences.bindReference(rtree.columnKeys(i), relation.output).eval(row).asInstanceOf[Number].doubleValue()
                val temp_point = new Point(tmp)

                var contain = true
                circle_query.foreach(item => {   //item: point, radius
                  if (temp_point.minDist(item._1) > item._2)
                    contain = false
                })
                queryMBR.contains(temp_point) && contain
              })
              sparkContext.parallelize(final_res, 1)
            }
          }).reduce((a, b) => a.union(b)).map(_.copy()).distinct()
        else
          rtree._indexedRDD.flatMap(_.data.iterator)
      }
      case other =>
        other._indexedRDD.flatMap(_.data.iterator)
    }
  }
}
