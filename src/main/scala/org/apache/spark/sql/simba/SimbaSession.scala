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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{Encoder, Row, SparkSession, DataFrame => SQLDataFrame, Dataset => SQLDataset}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.simba.index.IndexType
import org.apache.spark.util.Utils

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Created by dongx on 3/7/17.
  */
class SimbaSession private[simba] (@transient override val sparkContext: SparkContext)
  extends SparkSession(sparkContext) { self =>
  @transient
  private[sql] override lazy val sessionState: SimbaSessionState = {
    new SimbaSessionState(this)
  }

  def hasIndex(tableName: String, indexName: String): Boolean = {
    sessionState.indexManager.lookupIndexedData(table(tableName), indexName).nonEmpty
  }

  def indexTable(tableName: String, indexType: IndexType,
                 indexName: String, column: Array[String]): Unit = {
    val tbl = table(tableName)
    assert(tbl != null, "Table not found")
    val attrs = tbl.queryExecution.analyzed.output
    val columnKeys = column.map(attr => {
      var ans: Attribute = null
      for (i <- attrs.indices)
        if (attrs(i).name.equals(attr)) ans = attrs(i)
      assert(ans != null, "Attribute not found")
      ans
    }).toList
    sessionState.indexManager.createIndexQuery(table(tableName), indexType,
      indexName, columnKeys, Some(tableName))
  }

  def showIndex(tableName: String): Unit = sessionState.indexManager.showQuery(tableName)

  def persistIndex(indexName: String, fileName: String): Unit =
    sessionState.indexManager.persistIndex(this, indexName, fileName)

  def loadIndex(indexName: String, fileName: String): Unit =
    sessionState.indexManager.loadIndex(this, indexName, fileName)

  def dropIndexTableByName(tableName: String, indexName: String): Unit = {
    sessionState.indexManager.dropIndexByNameQuery(table(tableName), indexName)
  }

  def clearIndex(): Unit = sessionState.indexManager.clearIndex()

  object simbaImplicits extends Serializable {
    protected[simba] def _simbaContext: SparkSession = self

    implicit def datasetToSimbaDataSet[T : Encoder](ds: SQLDataset[T]): Dataset[T] =
      Dataset(self, ds.queryExecution.logical)

    implicit def dataframeToSimbaDataFrame(df: SQLDataFrame): DataFrame =
      Dataset.ofRows(self, df.queryExecution.logical)
  }
}

object SimbaSession {
  class Builder extends Logging {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    def appName(name: String): Builder = config("spark.app.name", name)

    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    def master(master: String): Builder = config("spark.master", master)

    def enableHiveSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SimbaSession with Hive support because " +
            "Hive classes are not found.")
      }
    }

    def getOrCreate(): SimbaSession = synchronized {
      // Get the session from current thread's active session.
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        options.foreach { case (k, v) => session.sessionState.setConf(k, v) }
        if (options.nonEmpty) {
          logWarning("Using an existing SimbaSession; some configuration may not take effect.")
        }
        return session
      }

      // Global synchronization so we will only set the default session once.
      SimbaSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          options.foreach { case (k, v) => session.sessionState.setConf(k, v) }
          if (options.nonEmpty) {
            logWarning("Using an existing SimbaSession; some configuration may not take effect.")
          }
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          val randomAppName = java.util.UUID.randomUUID().toString
          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(randomAppName)
          }
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SimbaSession
          options.foreach { case (k, v) => sc.conf.set(k, v) }
          if (!sc.conf.contains("spark.app.name")) {
            sc.conf.setAppName(randomAppName)
          }
          sc
        }
        session = new SimbaSession(sparkContext)
        options.foreach { case (k, v) => session.sessionState.setConf(k, v) }
        defaultSession.set(session)

        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            defaultSession.set(null)
            sqlListener.set(null)
          }
        })
      }

      return session
    }
  }

  def builder(): Builder = new Builder

  def setActiveSession(session: SimbaSession): Unit = {
    activeThreadSession.set(session)
  }

  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  def setDefaultSession(session: SimbaSession): Unit = {
    defaultSession.set(session)
  }

  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }

  private[sql] def getActiveSession: Option[SimbaSession] = Option(activeThreadSession.get)

  private[sql] def getDefaultSession: Option[SimbaSession] = Option(defaultSession.get)

  private[sql] val sqlListener = new AtomicReference[SQLListener]()

  private val activeThreadSession = new InheritableThreadLocal[SimbaSession]

  private val defaultSession = new AtomicReference[SimbaSession]

  private val HIVE_SESSION_STATE_CLASS_NAME = "org.apache.spark.sql.hive.HiveSessionState"

  private def sessionStateClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_SESSION_STATE_CLASS_NAME
      case "in-memory" => classOf[SessionState].getCanonicalName
    }
  }

  private def reflect[T, Arg <: AnyRef](
                                         className: String,
                                         ctorArg: Arg)(implicit ctorArgTag: ClassTag[Arg]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag.runtimeClass)
      ctor.newInstance(ctorArg).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      Utils.classForName(HIVE_SESSION_STATE_CLASS_NAME)
      Utils.classForName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

}
