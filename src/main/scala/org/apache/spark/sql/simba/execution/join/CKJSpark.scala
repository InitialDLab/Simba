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

package org.apache.spark.sql.simba.execution.join

import org.apache.spark.sql.simba.execution.SimbaPlan
import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.util.ShapeUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, Literal}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by dong on 1/20/16.
  * KNN Join based on Cartesian Product
  */
case class CKJSpark(left_key: Expression, right_key: Expression,
                    l: Literal, left: SparkPlan, right: SparkPlan) extends SimbaPlan {
  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def output: Seq[Attribute] = left.output ++ right.output

  final val k = l.value.asInstanceOf[Number].intValue()

  override protected def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute()
    val right_rdd = right.execute()

    left_rdd.map(row =>
      (ShapeUtils.getShape(left_key, left.output, row).asInstanceOf[Point], row)
    ).cartesian(right_rdd).map {
      case (l: (Point, InternalRow), r: InternalRow) =>
        val tmp_point = ShapeUtils.getShape(right_key, right.output, r).asInstanceOf[Point]
        l._2 -> List((tmp_point.minDist(l._1), r))
    }.reduceByKey {
      case (l_list: Seq[(Double, InternalRow)], r_list: Seq[(Double, InternalRow)]) =>
        (l_list ++ r_list).sortWith(_._1 < _._1).take(k)
    }.flatMapValues(list => list).mapPartitions { iter =>
      val joinedRow = new JoinedRow
      iter.map(r => joinedRow(r._1, r._2._2))
    }
  }

  override def children: Seq[SparkPlan] = Seq(left, right)
}
