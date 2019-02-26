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

package org.apache.spark.sql.simba.execution

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, QueryExecution => SQLQueryExecution}
import org.apache.spark.sql.simba.SimbaSession

/**
  * Created by dongx on 3/7/17.
  */
class QueryExecution(val simbaSession: SimbaSession, override val logical: LogicalPlan)
  extends SQLQueryExecution(simbaSession, logical) {

  lazy val withIndexedData: LogicalPlan = {
    assertAnalyzed()
    simbaSession.sessionState.indexManager.useIndexedData(withCachedData)
  }

  override lazy val optimizedPlan: LogicalPlan = {
    simbaSession.sessionState.optimizer.execute(withIndexedData)
  }

  override lazy val sparkPlan: SparkPlan ={
    SimbaSession.setActiveSession(simbaSession)
    simbaSession.sessionState.planner.plan(optimizedPlan).next()
  }

}
