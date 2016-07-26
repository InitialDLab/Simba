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
package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.spatial.Point

/**
  * Created by zhongpu on 16-7-20.
  */
object FetchPointUtils {

  def getFromRow(row: InternalRow, columns: List[Attribute], plan: LogicalPlan,
                 isPoint: Boolean): Point = {
    if (isPoint) {
      BindReferences.bindReference(columns.head, plan.output).eval(row)
        .asInstanceOf[Point]
    } else {
      Point(columns.toArray.map(BindReferences.bindReference(_, plan.output).eval(row)
        .asInstanceOf[Number].doubleValue()))
    }
  }

  def getFromRow(row: InternalRow, columns: List[Attribute], plan: SparkPlan,
                 isPoint: Boolean): Point = {
    if (isPoint) {
      BindReferences.bindReference(columns.head, plan.output).eval(row)
        .asInstanceOf[Point]
    } else {
      Point(columns.toArray.map(BindReferences.bindReference(_, plan.output).eval(row)
        .asInstanceOf[Number].doubleValue()))
    }
  }
}
