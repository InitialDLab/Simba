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

import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.util.{NumberUtil, ShapeUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, Literal}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by dong on 1/20/16.
  * Distance Join based on Cartesian Product
  */
case class CDJSpark(left_key: Expression, right_key: Expression,
                    l: Literal, left: SparkPlan, right: SparkPlan) extends SparkPlan {
  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def output: Seq[Attribute] = left.output ++ right.output

  final val r = NumberUtil.literalToDouble(l)

  override protected def doExecute(): RDD[InternalRow] =
    left.execute().cartesian(right.execute()).mapPartitions { iter =>
      val joinedRow = new JoinedRow
      iter.filter { row =>
        val point1 = ShapeUtils.getShape(left_key, left.output, row._1).asInstanceOf[Point]
        val point2 = ShapeUtils.getShape(right_key, right.output, row._2).asInstanceOf[Point]
        point1.minDist(point2) <= r
      }.map(row => joinedRow(row._1, row._2))
    }

  override def children: Seq[SparkPlan] = Seq(left, right)
}
