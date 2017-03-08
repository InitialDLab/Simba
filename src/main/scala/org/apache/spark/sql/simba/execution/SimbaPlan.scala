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

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.simba.SimbaSession

/**
  * Created by dongx on 3/7/17.
  */
abstract class SimbaPlan extends SparkPlan {

  @transient
  protected[simba] final val simbaSessionState = SimbaSession.getActiveSession.map(_.sessionState).orNull

  protected override def sparkContext = SimbaSession.getActiveSession.map(_.sparkContext).orNull
}
