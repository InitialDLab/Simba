package org.apache.spark.sql.execution.joins

/**
 * Created by oizz01 on 4/19/15.
 * Changed by crystalove on 5/25/15
 */
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.spatial.Point
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}

/**
 * :: Developer API ::
 * knn join
 */
@DeveloperApi
case class CartesianKNNJoinExecution(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    kNN: Literal,
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode {
  override def output = left.output ++ right.output

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil


  final val dimension = leftKeys.length
  final val k = kNN.value.toString.trim.toInt

  override def execute() = {
    val leftRDD = left.execute()
    val rightRDD = right.execute()

    leftRDD.map {
      row =>
        val x = BindReferences.bindReference(leftKeys.head, left.output).eval(row).asInstanceOf[Number].doubleValue()
        val y = BindReferences.bindReference(leftKeys(1), left.output).eval(row).asInstanceOf[Number].doubleValue()
        (new Point(Array(x, y)), row)
    }.cartesian(rightRDD).map({
      case (l: (Point, Row), r: Row) =>
        val current_x = BindReferences.bindReference(rightKeys.head, right.output).eval(r).asInstanceOf[Number].doubleValue()
        val current_y = BindReferences.bindReference(rightKeys(1), right.output).eval(r).asInstanceOf[Number].doubleValue()
        val distance = new Point(Array(current_x, current_y)).minDist(l._1)
        l._2 -> List((distance, r))
    }).reduceByKey({
      case (l_list: Seq[(Double, Row)], r_list: Seq[(Double, Row)]) =>
        (l_list ++ r_list).sortWith(_._1 < _._1).take(k)
    }).flatMapValues(list => list).mapPartitions { iter =>
      val joinedRow = new JoinedRow
      iter.map(r => joinedRow(r._1, r._2._2))
    }
  }
}
