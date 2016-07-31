/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.spark.sql.execution.datasources.PartitionedTriple

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType

/**
  * Created by gefei on 16-7-31.
  */
//class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister{
//  override def shortName(): String = "spatial"
//
//  override def createRelation(
//     sqlContext: SQLContext,
//     paths: Array[String],
//     dataSchema: Option[StructType],
//     partitionColumns: Option[StructType],
//     parameters: Map[String, String]): HadoopFsRelation = {
//
//    new JSONRelation(
//      inputRDD = None,
//      maybeDataSchema = dataSchema,
//      maybePartitionSpec = None,
//      userDefinedPartitionColumns = partitionColumns,
//      paths = paths,
//      parameters = parameters)(sqlContext)
//  }
//}


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "partitioned-triple"

  override def createRelation(sqlContext: SQLContext,
            parameters: Map[String, String]): BaseRelation = {
    val path = parameters.get("path")
    path match {
      case Some(p) => new PartitionedTripleRelation(sqlContext, p)
      case _ =>
        throw new IllegalArgumentException("Path is required for PartitionedTriple datasets")
    }
  }
}

class PartitionedTripleRelation(val sqlContext: SQLContext, path: String) extends BaseRelation
  with PrunedFilteredScan with Logging {

  def inputFiles: Array[String] = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    .listStatus(new Path(path)).map({a => a.getPath.getName})

  override def schema: StructType = StructType(
    StructField("tag", StringType, false) ::
      StructField("timestamp", LongType, false) ::
      StructField("value", DoubleType, false) :: Nil )

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo(s"Building scan for path: $path")
    var tags: Set[String] = inputFiles.map({a => a.substring(0, a.length - 4)}).toSet
    logInfo(s"Found ${tags.size} tags from path.")

    // Because of our naming scheme we can only do stuff with the tag related
    // filters. All the filters will still be applied at execution so we can
    // safely ignore the others.
    filters.foreach {
        case EqualTo(attr, value) =>
          tags = tags filter {a => attr != "tag" || a == value}
        case In(attr, values) =>
          tags = tags filter {a => attr != "tag" || values.toSet.contains(a)}
        case StringStartsWith(attr, value) =>
          tags = tags filter {a => attr != "tag" || a.startsWith(value)}
        case StringEndsWith(attr, value) =>
          tags = tags filter {a => attr != "tag" || a.endsWith(value)}
        case StringContains(attr, value) =>
          tags = tags filter {a => attr != "tag" || a.contains(value)}
    }
    val paths = tags.map {a => new Path(path, a + ".csv").toString()}
    logInfo(s"Will read ${tags.size} files based on filters provided.")

    val rawRdd = sqlContext.sparkContext.textFile(paths.mkString(","))
    val parsedRdd = rawRdd.map { r =>
      val split = r.split(",")
      val tag = split(0)
      val timestamp = split(1).toLong
      val value = split(2).toDouble
      val rowSeq = requiredColumns.map {
          case "tag" => tag
          case "timestamp" => timestamp
          case "value" => value
      }
      Row.fromSeq(rowSeq)
    }
    parsedRdd
  }
}

