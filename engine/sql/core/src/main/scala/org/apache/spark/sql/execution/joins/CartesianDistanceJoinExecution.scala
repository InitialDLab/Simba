package org.apache.spark.sql.execution.joins

/**
 * Created by oizz01 on 4/21/15.
 * Changed by crystalove on 5/25/15
 * Distance Join Executor
 */
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Partitioning}
import org.apache.spark.sql.spatial.Point
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.Filter

import scala.collection.mutable

/**
 * :: Developer API ::
 * distance join
 */
@DeveloperApi
case class CartesianDistanceJoinExecution(
                                           leftKeys: Seq[Expression],
                                           rightKeys: Seq[Expression],
                                           r: Literal,
                                           left: SparkPlan,
                                           right: SparkPlan)
  extends BinaryNode {
  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def output = left.output ++ right.output

  final val dimension = leftKeys.length
  final val radius = r.value.toString.trim.toDouble

  override def execute() =
    left.execute().cartesian(right.execute()).mapPartitions {
      iter => {
        val joinedRow = new JoinedRow
        iter.filter { row => {
          val x1 = BindReferences.bindReference(rightKeys.head, right.output).eval(row._2).asInstanceOf[Number].doubleValue
          val y1 = BindReferences.bindReference(rightKeys(1), right.output).eval(row._2).asInstanceOf[Number].doubleValue
          val x2 = BindReferences.bindReference(leftKeys.head, left.output).eval(row._1).asInstanceOf[Number].doubleValue
          val y2 = BindReferences.bindReference(leftKeys(1), left.output).eval(row._1).asInstanceOf[Number].doubleValue
          new Point(Array(x1, y1)).minDist(new Point(Array(x2, y2))) <= radius
        }
        }.map(row => joinedRow(row._1, row._2))
      }
    }
}