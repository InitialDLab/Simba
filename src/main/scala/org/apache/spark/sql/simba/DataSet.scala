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

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Dataset => SQLDataSet}
import org.apache.spark.sql.simba.execution.QueryExecution

/**
  * Created by dongx on 3/7/17.
  */
class DataSet[T] private[simba] (@transient val simbaSession: SimbaSession,
                                 @transient override val queryExecution: QueryExecution,
                                 encoder: Encoder[T])
  extends SQLDataSet[T](simbaSession, queryExecution, encoder) {

  def this(simbaSession: SimbaSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(simbaSession, {
      val qe = simbaSession.sessionState.executePlan(logicalPlan)
      qe
    }, encoder)
  }
}
