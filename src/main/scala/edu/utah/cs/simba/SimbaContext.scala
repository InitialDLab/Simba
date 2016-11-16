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
 *
 */

package edu.utah.cs.simba

import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import edu.utah.cs.simba.execution.SimbaPlanner
import edu.utah.cs.simba.index.IndexType
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{SQLContext, DataFrame => SQLDataFrame, execution => sparkexecution}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.language.implicitConversions

/**
  * Created by dongx on 11/11/16.
  */
class SimbaContext private[simba](@transient val sc: SparkContext,
                                  @transient protected[simba] val indexManager: IndexManager) extends SQLContext(sc) {
  self =>
  def this(sparkContext: SparkContext) = {
    this(sparkContext, new IndexManager)
  }
  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  protected[simba] lazy val simbaConf = new SimbaConf

  protected[simba] def getSQLOptimizer = optimizer

  protected[simba] lazy val simbaOptimizer: SimbaOptimizer = new SimbaOptimizer

  protected[simba] val simbaPlanner: sparkexecution.SparkPlanner = new SimbaPlanner(this)

  override def setConf(props: Properties): Unit = {
    props.asScala.foreach { case (k, v) => setConf(k, v) }
  }

  override def setConf(key: String, value: String): Unit = {
    if (key.startsWith("simba.")) simbaConf.setConfString(key, value)
    else conf.setConfString(key, value)
  }

  override def executePlan(plan: LogicalPlan) =
    new execution.QueryExecution(this, plan)

  override def getConf(key: String): String = {
    if (key.startsWith("simba.")) simbaConf.getConfString(key)
    else conf.getConfString(key)
  }

  override def getConf(key: String, defaultValue: String): String = {
    if (key.startsWith("simba.")) conf.getConfString(key, defaultValue)
    else conf.getConfString(key, defaultValue)
  }

  override def getAllConfs: immutable.Map[String, String] = {
    conf.getAllConfs ++ simbaConf.getAllConfs
  }

  override def newSession(): SimbaContext = {
    new SimbaContext(sc = sc, indexManager = indexManager)
  }

  def hasIndex(tableName: String, indexName: String): Boolean = {
    import SimbaImplicits._
    indexManager.lookupIndexedData(table(tableName), indexName).nonEmpty
  }

  def indexTable(tableName: String, indexType: IndexType,
                 indexName: String, column: Array[String]): Unit = {
    import SimbaImplicits._
    val table = _simbaContext.table(tableName)
    assert(table != null, "Table not found")
    val attrs = table.queryExecution.analyzed.output
    val columnKeys = column.map(attr => {
      var ans: Attribute = null
      for (i <- attrs.indices)
        if (attrs(i).name.equals(attr)) ans = attrs(i)
      assert(ans != null, "Attribute not found")
      ans
    }).toList
    indexManager.createIndexQuery(_simbaContext.table(tableName), indexType,
      indexName, columnKeys, Some(tableName))
  }

  def showIndex(tableName: String): Unit = indexManager.showQuery(this, tableName)

  def persistIndex(indexName: String, fileName: String): Unit =
    indexManager.persistIndex(this, indexName, fileName)

  def loadIndex(indexName: String, fileName: String): Unit =
    indexManager.loadIndex(this, indexName, fileName)

  def dropIndexTableByName(tableName: String, indexName: String): Unit = {
    import SimbaImplicits._
    indexManager.dropIndexByNameQuery(table(tableName), indexName)
  }

  def clearIndex(): Unit = indexManager.clearIndex()

  object SimbaImplicits extends Serializable {
    protected[simba] def _simbaContext: SimbaContext = self

    implicit def dfToSimbaDF(df: SQLDataFrame): DataFrame = DataFrame(self, df.queryExecution.logical)
  }
}

object SimbaContext {
  private val activeContext: InheritableThreadLocal[SimbaContext] = new InheritableThreadLocal[SimbaContext]

  @transient private val instantiatedContext = new AtomicReference[SimbaContext]()

  def getOrCreate(sparkContext: SparkContext): SimbaContext = {
    val ctx = activeContext.get()
    if (ctx != null && !ctx.sparkContext.isStopped) {
      return ctx
    }

    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null || ctx.sparkContext.isStopped) {
        new SimbaContext(sparkContext)
      } else {
        ctx
      }
    }
  }

  private[simba] def clearInstantiatedContext(): Unit = {
    instantiatedContext.set(null)
  }

  private[simba] def setInstantiatedContext(simbaContext: SimbaContext): Unit = {
    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null || ctx.sparkContext.isStopped) {
        instantiatedContext.set(simbaContext)
      }
    }
  }

  private[simba] def getInstantiatedContextOption: Option[SimbaContext] = {
    Option(instantiatedContext.get())
  }

  def setActive(simbaContext: SimbaContext): Unit = {
    activeContext.set(simbaContext)
  }

  def clearActive(): Unit = {
    activeContext.remove()
  }

  private[simba] def getActive: Option[SimbaContext] = {
    Option(activeContext.get())
  }
}