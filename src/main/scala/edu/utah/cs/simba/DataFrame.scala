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

package edu.utah.cs.simba

import edu.utah.cs.simba.execution.QueryExecution
import edu.utah.cs.simba.expression._
import edu.utah.cs.simba.index.IndexType
import edu.utah.cs.simba.plans.{DistanceJoin, KNNJoin, SpatialJoin}
import edu.utah.cs.simba.spatial.Point
import edu.utah.cs.simba.util.{LiteralUtil, PointFromCoords}
import org.apache.spark.sql.{DataFrame => SQLDataFrame}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.storage.StorageLevel

/**
  * Created by dongx on 11/14/2016.
  */
private[simba] object DataFrame {
  def apply(simbaContext: SimbaContext, logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(simbaContext, logicalPlan)
  }
}

class DataFrame private[simba](
    @transient val simbaContext: SimbaContext,
    @transient override val queryExecution: QueryExecution)
  extends SQLDataFrame(simbaContext, queryExecution.logical) {

  def this(simbaContext: SimbaContext, logicalPlan: LogicalPlan) {
    this(simbaContext, {
      val qe = simbaContext.executePlan(logicalPlan)
      if (simbaContext.getConf("spark.sql.eagerAnalysis") == "true") {
        qe.assertAnalyzed()
      }
      qe
    })
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
  def circleRange(key: String, point: PointFromCoords, r: Double): DataFrame = withPlan {
    val attrs = getAttributes(Array(key))
    assert(attrs.head != null, "column not found")

    Filter(InCircleRange(attrs.head,
      LiteralUtil(new Point(point.coords.toArray)),
      LiteralUtil(r)), logicalPlan)
  }

  /**
    * Spatial operation DistanceJoin
    */
  def distanceJoin(right: DataFrame, leftKeys: Array[String],
                   rightKeys: Array[String], r: Double) : DataFrame = withPlan {
    val leftAttrs = getAttributes(leftKeys)
    val rightAttrs = getAttributes(rightKeys, right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, DistanceJoin,
      Some(InCircleRange(PointWrapper(rightAttrs),
        PointWrapper(leftAttrs),
        LiteralUtil(r))))
  }

  def distanceJoin(right: DataFrame, leftKey: String,
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
  def knnJoin(right: DataFrame, leftKeys: Array[String],
              rightKeys: Array[String], k : Int) : DataFrame = withPlan {
    val leftAttrs = getAttributes(leftKeys)
    val rightAttrs = getAttributes(rightKeys, right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, KNNJoin,
      Some(InKNN(PointWrapper(rightAttrs),
        PointWrapper(leftAttrs), LiteralUtil(k))))
  }

  def knnJoin(right: DataFrame, leftKey: String,
              rightKey: String, k : Int) : DataFrame = withPlan {
    val leftAttrs = getAttributes(Array(leftKey))
    val rightAttrs = getAttributes(Array(rightKey), right.queryExecution.analyzed.output)
    SpatialJoin(this.logicalPlan, right.logicalPlan, KNNJoin,
      Some(InKNN(rightAttrs.head,
        leftAttrs.head, LiteralUtil(k))))
  }

  private def getAttributes(keys: Array[String], attrs: Seq[Attribute] = this.queryExecution.analyzed.output)
  : Array[Attribute] = {
    keys.map(key => {
      val temp = attrs.indexWhere(_.name == key)
      if (temp >= 0) attrs(temp)
      else null
    })
  }

  /////////////////////////////////////////////////////////////////////////////
  // Index operations
  /////////////////////////////////////////////////////////////////////////////
  /**
    * @group extended
    */
  def index(indexType: IndexType, indexName: String, column: List[Attribute]): this.type = {
    simbaContext.indexManager.createIndexQuery(this, indexType, indexName, column)
    this
  }

  /**
    * @group extended
    */
  def setStorageLevel(indexName: String, level: StorageLevel): this.type = {
    simbaContext.indexManager.setStorageLevel(this, indexName, level)
    this
  }

  /**
    * @group extended
    */
  def dropIndex(blocking: Boolean): this.type = {
    simbaContext.indexManager.tryDropIndexQuery(this, blocking)
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
    simbaContext.indexManager.tryDropIndexByNameQuery(this, indexName, blocking = false)
    this
  }

  /**
    * @group extended
    */
  def persistIndex(indexName: String, fileName: String): this.type = {
    simbaContext.indexManager.persistIndex(this.simbaContext, indexName, fileName)
    this
  }

  /**
    * @group extended
    */
  def loadIndex(indexName: String, fileName: String): this.type = {
    simbaContext.indexManager.loadIndex(this.simbaContext, indexName, fileName)
    this
  }


  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    new DataFrame(simbaContext, logicalPlan)
  }
}
