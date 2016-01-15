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


import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.index._
import org.apache.spark.sql.types.StringType
import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._


/**
 * The top level Spark SQL parser. This parser recognizes syntaxes that are available for all SQL
 * dialects supported by Spark SQL, and delegates all the other syntaxes to the `fallback` parser.
 *
 * @param fallback A function that parses an input string to a logical plan
 */
private[sql] class SparkSQLParser(fallback: String => LogicalPlan) extends AbstractSparkSQLParser {

  // A parser for the key-value part of the "SET [key = [value ]]" syntax
  private object SetCommandParser extends RegexParsers {
    private val key: Parser[String] = "(?m)[^=]+".r

    private val value: Parser[String] = "(?m).*$".r

    private val output: Seq[Attribute] = Seq(AttributeReference("", StringType, nullable = false)())

    private val pair: Parser[LogicalPlan] =
      (key ~ ("=".r ~> value).?).? ^^ {
        case None => SetCommand(None, output)
        case Some(k ~ v) => SetCommand(Some(k.trim -> v.map(_.trim)), output)
      }

    def apply(input: String): LogicalPlan = parseAll(pair, input) match {
      case Success(plan, _) => plan
      case x => sys.error(x.toString)
    }
  }

    protected val AS      = Keyword("AS")
    protected val CACHE   = Keyword("CACHE")
    protected val CLEAR   = Keyword("CLEAR")
    protected val CREATE   = Keyword("CREATE")
    protected val DEINDEX = Keyword("DEINDEX")
    protected val FROM    = Keyword("FROM")
    protected val HASHMAP = Keyword("HASHMAP")
    protected val IN      = Keyword("IN")
    protected val INDEX   = Keyword("INDEX")
    protected val LAZY    = Keyword("LAZY")
    protected val LOAD    = Keyword("LOAD")
    protected val ON      = Keyword("ON")
    protected val PERSIST = Keyword("PERSIST")
    protected val RTREE   = Keyword("RTREE")
    protected val SET     = Keyword("SET")
    protected val SHOW    = Keyword("SHOW")
    protected val TABLE   = Keyword("TABLE")
    protected val TABLES  = Keyword("TABLES")
    protected val TREEMAP = Keyword("TREEMAP")
    protected val UNCACHE = Keyword("UNCACHE")
    protected val USE     = Keyword("USE")

    override protected lazy val start: Parser[LogicalPlan] = index | deindex | persistIndex | loadIndex | cache | uncache | set | show | others

    private lazy val index: Parser[LogicalPlan] =
    (CREATE ~> INDEX ~> ident) ~ (ON ~> ident ~ ("(" ~> repsep(ident, ",") <~ ")")) ~ (USE ~> indexType) ^^ {
        case indexName ~ (tableName ~ columnName) ~ index_type =>
            IndexTableCommand(tableName, columnName, index_type, indexName)
    }

    protected lazy val indexType: Parser[IndexType] =
    ( RTREE           ^^^ RTreeType
    | TREEMAP       ^^^ TreeMapType
    | HASHMAP       ^^^ HashMapType
    )

    private lazy val deindex: Parser[LogicalPlan] = (
    DEINDEX ~> ident ~ (ON ~> ident) ^^ {
        case indexName ~ tableName => DeindexTableByNameCommand(tableName, indexName)
    }
    | CLEAR ~> INDEX ~> (ON ~> ident) ^^ {
        case tableName => DeindexTableCommand(tableName)
    }
    | CLEAR ~> INDEX ^^^ ClearIndexCommand
    )

    private lazy val persistIndex: Parser[LogicalPlan] =
    PERSIST ~> ident ~ (IN ~> restInput) ^^ {
        case indexName ~ fileName => PersistIndexCommand(indexName, fileName)
    }

    private lazy val loadIndex: Parser[LogicalPlan] =
    (LOAD ~> INDEX ~> ident) ~ (IN ~> restInput) ^^ {
        case indexName ~ fileName => LoadIndexCommand(indexName, fileName)
    }

  private lazy val cache: Parser[LogicalPlan] =
    CACHE ~> LAZY.? ~ (TABLE ~> ident) ~ (AS ~> restInput).? ^^ {
      case isLazy ~ tableName ~ plan =>
        CacheTableCommand(tableName, plan.map(fallback), isLazy.isDefined)
    }

  private lazy val uncache: Parser[LogicalPlan] =
    ( UNCACHE ~ TABLE ~> ident ^^ {
        case tableName => UncacheTableCommand(tableName)
      }
    | CLEAR ~ CACHE ^^^ ClearCacheCommand
    )

  private lazy val set: Parser[LogicalPlan] =
    SET ~> restInput ^^ {
      case input => SetCommandParser(input)
    }

  private lazy val show: Parser[LogicalPlan] = (
    SHOW ~> TABLES ~ (IN ~> ident).? ^^ {
      case _ ~ dbName => ShowTablesCommand(dbName)
    }
    | SHOW ~> INDEX ~> ON ~> ident ^^ {
      case tableName => ShowIndexCommand(tableName)
    }
  )

  private lazy val others: Parser[LogicalPlan] =
    wholeInput ^^ {
      case input => fallback(input)
    }

}
