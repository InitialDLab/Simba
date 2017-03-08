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

package org.apache.spark.sql.simba.util

import org.apache.spark.sql.simba.ShapeType
import org.apache.spark.sql.simba.spatial.Shape
import org.apache.spark.sql.catalyst.expressions.Literal

/**
  * Created by dongx on 11/14/2016.
  */
object LiteralUtil {
  def apply(v: Any): Literal = v match {
    case s: Shape => Literal.create(v, ShapeType)
    case _ => Literal(v)
  }
}
