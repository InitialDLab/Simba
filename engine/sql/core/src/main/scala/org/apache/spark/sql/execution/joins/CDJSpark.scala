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

package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.spatial._

/**
  * Created by dong on 1/20/16.
  * Distance Join based on Cartesian Product
  */
case class CDJSpark(left_key: Expression,
                    right_key: Expression,
                    l: Literal,
                    left: SparkPlan,
                    right: SparkPlan) extends BinaryNode {
  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def output: Seq[Attribute] = left.output ++ right.output

  final val r = NumberConverter.literalToDouble(l)

  override protected def doExecute(): RDD[InternalRow] =
    left.execute().cartesian(right.execute()).mapPartitions { iter =>
      val joinedRow = new JoinedRow
      iter.filter { row =>
        val point1 = BindReferences.bindReference(left_key, left.output).eval(row._1).
          asInstanceOf[Point]
        val point2 = BindReferences.bindReference(right_key, right.output).eval(row._2).
          asInstanceOf[Point]
        point1.minDist(point2) <= r
      }.map(row => joinedRow(row._1, row._2))
    }
}
