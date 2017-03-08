/*
 * Copyright 2017 by Simba Project
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
 *
 */

package org.apache.spark.sql.simba

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.{Encoder, Row, Dataset => SQLDataset}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.simba.execution.QueryExecution
import org.apache.spark.sql.simba.expression._
import org.apache.spark.sql.simba.index.IndexType
import org.apache.spark.sql.simba.plans._
import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.util.LiteralUtil
import org.apache.spark.storage.StorageLevel

/**
  * Created by dongx on 3/7/17.
  */


class Dataset[T] private[simba] (@transient val simbaSession: SimbaSession,
                                 @transient override val queryExecution: QueryExecution,
                                 encoder: Encoder[T])
  extends SQLDataset[T](simbaSession, queryExecution.logical, encoder) {

  def this(simbaSession: SimbaSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(simbaSession, {
      val qe = simbaSession.sessionState.executePlan(logicalPlan)
      qe
    }, encoder)
  }

  /**
    * Spatial operation, range query.
    * {{{
    *   point.range(Array("x", "y"), Array(10, 10), Array(20, 20))
    *   point.filter($"x" >= 10 && $"x" <= 20 && $"y" >= 10 && $"y" <= 20)
    * }}}
    */
  def range(keys: Array[String], point1: Array[Double], point2: Array[Double]): DataFrame = withPlan {
    val attrs = getAttributes(keys)
    attrs.foreach(attr => assert(attr != null, "column not found"))

    Filter(InRange(PointWrapper(attrs),
      LiteralUtil(new Point(point1)),
      LiteralUtil(new Point(point2))), logicalPlan)
  }

  /**
    * Spatial operation, range query
    * {{{
    *   point.range(p, Array(10, 10), Array(20, 20))
    * }}}
    */
  def range(key: String, point1: Array[Double], point2: Array[Double]): DataFrame = withPlan {
    val attrs = getAttributes(Array(key))
    assert(attrs.head != null, "column not found")

    Filter(InRange(attrs.head,
      LiteralUtil(new Point(point1)),
      LiteralUtil(new Point(point2))), logicalPlan)
  }

  /**
    * Spatial operation knn
    * Find k nearest neighbor of a given point
    */
  def knn(keys: Array[String], point: Array[Double], k: Int): DataFrame = withPlan{
    val attrs = getAttributes(keys)
    attrs.foreach(attr => assert(attr != null, "column not found"))
    Filter(InKNN(PointWrapper(attrs),
      LiteralUtil(new Point(point)), LiteralUtil(k)), logicalPlan)
  }

  def knn(key: String, point: Array[Double], k: Int): DataFrame = withPlan{
    val attrs = getAttributes(Array(key))
    assert(attrs.head != null, "column not found")
    Filter(InKNN(attrs.head,
      LiteralUtil(new Point(point)), LiteralUtil(k)), logicalPlan)
  }

  /**
    * Spatial operation circle range query
    * {{{
    *   point.circleRange(Array("x", "y"), Array(10, 10), 5)
    *   point.filter(($"x" - 10) * ($"x" - 10) + ($"y" - 10) * ($"y" - 10) <= 5 * 5)
    * }}}
    */
  def circleRange(keys: Array[String], point: Array[Double], r: Double): DataFrame = withPlan {
    val attrs = getAttributes(keys)
    attrs.foreach(attr => assert(attr != null, "column not found"))
    Filter(InCircleRange(PointWrapper(attrs),
      LiteralUtil(new Point(point)),
      LiteralUtil(r)), logicalPlan)
  }

  /**
    * Spatial operation circle range query
    * {{{
    *   point.circleRange(p, Point(10, 10), 5)
    * }}}
    */
  def circleRange(key: String, point: Array[Double], r: Double): DataFrame = withPlan {
    val attrs = getAttributes(Array(key))
    assert(attrs.head != null, "column not found")

    Filter(InCircleRange(attrs.head,
      LiteralUtil(new Point(point)),
      LiteralUtil(r)), logicalPlan)
  }

  /**
    * Spatial operation DistanceJoin
    */
  def distanceJoin(right: Dataset[_], leftKeys: Array[String],
                   rightKeys: Array[String], r: Double) : DataFrame = withPlan {
    val leftAttrs = getAttributes(leftKeys)
    val rightAttrs = getAttributes(rightKeys, right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, DistanceJoin,
      Some(InCircleRange(PointWrapper(rightAttrs),
        PointWrapper(leftAttrs),
        LiteralUtil(r))))
  }

  def distanceJoin(right: Dataset[_], leftKey: String,
                   rightKey: String, r: Double) : DataFrame = withPlan {
    val leftAttrs = getAttributes(Array(leftKey))
    val rightAttrs = getAttributes(Array(rightKey), right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, DistanceJoin,
      Some(InCircleRange(rightAttrs.head,
        leftAttrs.head,
        LiteralUtil(r))))
  }

  /**
    * Spatial operation KNNJoin
    */
  def knnJoin(right: Dataset[_], leftKeys: Array[String],
              rightKeys: Array[String], k : Int) : DataFrame = withPlan {
    val leftAttrs = getAttributes(leftKeys)
    val rightAttrs = getAttributes(rightKeys, right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, KNNJoin,
      Some(InKNN(PointWrapper(rightAttrs),
        PointWrapper(leftAttrs), LiteralUtil(k))))
  }

  def knnJoin(right: Dataset[_], leftKey: String,
              rightKey: String, k : Int) : DataFrame = withPlan {
    val leftAttrs = getAttributes(Array(leftKey))
    val rightAttrs = getAttributes(Array(rightKey), right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, KNNJoin,
      Some(InKNN(rightAttrs.head,
        leftAttrs.head, LiteralUtil(k))))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Index operations
  /////////////////////////////////////////////////////////////////////////////
  /**
    * @group extended
    */
  def index(indexType: IndexType, indexName: String, column: Array[String]): this.type = {
    simbaSession.sessionState.indexManager.createIndexQuery(this, indexType, indexName, getAttributes(column).toList)
    this
  }

  /**
    * @group extended
    */
  def setStorageLevel(indexName: String, level: StorageLevel): this.type = {
    simbaSession.sessionState.indexManager.setStorageLevel(this, indexName, level)
    this
  }

  /**
    * @group extended
    */
  def dropIndex(blocking: Boolean): this.type = {
    simbaSession.sessionState.indexManager.tryDropIndexQuery(this, blocking)
    this
  }

  /**
    * @group extended
    */
  def dropIndex(): this.type = dropIndex(blocking = false)

  /**
    * @group extended
    */
  def dropIndexByName(indexName : String) : this.type = {
    simbaSession.sessionState.indexManager.tryDropIndexByNameQuery(this, indexName, blocking = false)
    this
  }

  /**
    * @group extended
    */
  def persistIndex(indexName: String, fileName: String): this.type = {
    simbaSession.sessionState.indexManager.persistIndex(this.simbaSession, indexName, fileName)
    this
  }

  /**
    * @group extended
    */
  def loadIndex(indexName: String, fileName: String): this.type = {
    simbaSession.sessionState.indexManager.loadIndex(this.simbaSession, indexName, fileName)
    this
  }


  private def getAttributes(keys: Array[String], attrs: Seq[Attribute] = this.queryExecution.analyzed.output)
  : Array[Attribute] = {
    keys.map(key => {
      val temp = attrs.indexWhere(_.name == key)
      if (temp >= 0) attrs(temp)
      else null
    })
  }

  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    Dataset.ofRows(simbaSession, logicalPlan)
  }
}

private[simba] object Dataset {
  def apply[T: Encoder](simbaSession: SimbaSession, logicalPlan: LogicalPlan): Dataset[T] = {
    new Dataset(simbaSession, logicalPlan, implicitly[Encoder[T]])
  }

  def ofRows(simbaSession: SimbaSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = simbaSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](simbaSession, qe, RowEncoder(qe.analyzed.schema))
  }
}
