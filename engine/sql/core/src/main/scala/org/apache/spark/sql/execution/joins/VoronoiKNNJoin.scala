package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SparkPlan, BinaryNode}
import org.apache.spark.sql.partitioner.{voronoiPartition, mapDPartition}
import org.apache.spark.sql.spatial.Point
import org.apache.spark.util.BoundedPriorityQueue

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks

case class VoronoiKNNJoin(leftKeys: Seq[Expression],
                          rightKeys: Seq[Expression],
                          kNN: Literal,
                          left: SparkPlan,
                          right: SparkPlan)
  extends BinaryNode{
  override def output = left.output ++ right.output

  //Parameters that set in sqlContext.conf
  final val num_partitions = sqlContext.conf.numShufflePartitions
  final val num_of_pivot_sets = sqlContext.conf.voronoiPivotsetSize
  final val num_of_pivots = num_partitions * sqlContext.conf.thetaBoost
  final val k = kNN.toString.toInt
  final val dimension = leftKeys.length

  case class LeftMetaInfo(partitionId: Int, upperBound: Double, var theta : Double = Double.MaxValue)
  case class RightMetaInfo(partitionId: Int, upperBound: Double, knnOfPivot: Array[Double])

  def generatePivots(originRDD: RDD[Point], num_pivots : Int) = {
    val sampled = originRDD.takeSample(withReplacement = false, num_of_pivot_sets * num_pivots, System.currentTimeMillis())
    val shuffled = Random.shuffle(sampled.toList).toArray
    var best = 0.0
    var best_offset = -1
    for (offset <- 0.to(shuffled.length - 1, num_of_pivots)) {
      var sum = 0.0
      for (i <- offset to (offset + num_of_pivots - 1))
        for (j <- (i + 1) to (offset + num_of_pivots - 1))
          sum += shuffled(i).minDist(shuffled(j))
      if (best < sum) {
        best = sum
        best_offset = offset
      }
    }
    shuffled.slice(best_offset, best_offset + num_of_pivots)
  }

  def geoGrouping(pivots: Array[Point], cell_size: Array[Int], num_group: Int) = {
    assert(pivots.length == cell_size.length)
    assert(pivots.length >= num_group)
    val remaining = new mutable.HashSet[Int]
    for (i <- pivots.indices) remaining += i
    var best = 0.0
    var best_id = -1
    remaining.foreach(x => {
      var tmp = 0.0
      for (j <- pivots.indices)
        tmp += pivots(x).minDist(pivots(j))
      if (tmp > best) {
        best = tmp
        best_id = x
      }
    })

    val group_base = new Array[Int](num_group)
    class SizeOrdering extends Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = - x._2.compare(y._2)
    }
    val group_size = new mutable.PriorityQueue[(Int, Int)]()(new SizeOrdering)
    val grouping = new Array[Array[Int]](num_group)
    val pivot_to_group = new Array[Int](pivots.length)
    group_base(0) = best_id
    remaining -= best_id

    for (i <- 1 to num_group - 1) {
      var best = 0.0
      var best_id = -1
      remaining.foreach(x => {
        var tmp = 0.0
        for (j <- 0 to i - 1)
          tmp += pivots(x).minDist(pivots(group_base(j)))
        if (tmp > best) {
          best = tmp
          best_id = x
        }
      })
      group_base(i) = best_id
      remaining -= best_id
    }

    for (i <- group_base.indices) {
      group_size += ((i, cell_size(group_base(i))))
      grouping(i) = Array(group_base(i))
      pivot_to_group(group_base(i)) = i
    }

    while (remaining.nonEmpty) {
      val now = group_size.dequeue()
      var best = Double.MaxValue
      var best_id = -1
      remaining.foreach(x => {
        var tmp = 0.0
        grouping(now._1).foreach(p => tmp += pivots(p).minDist(pivots(x)))
        if (tmp < best) {
          best = tmp
          best_id = x
        }
      })
      grouping(now._1) = grouping(now._1) :+ best_id
      pivot_to_group(best_id) = now._1
      group_size += ((now._1, now._2 + cell_size(best_id)))
      remaining -= best_id
    }

    (grouping, pivot_to_group)
  }

  def knnObjectForSinglePoint(leftItem: (Point, Row),
                              leftPid: Int,
                              sortByDist: IndexedSeq[(Int, Double)],
                              pivots: Array[Point],
                              rightMetaInfo: Array[RightMetaInfo],
                              rightDataByPivot: Array[Array[(Point, Row, Double)]]): Iterable[Row] = {
    val leftPoint = leftItem._1
    val leftRow = leftItem._2

    val distToPivot: Array[Double] = new Array[Double](num_of_pivots)

    for (i <- distToPivot.indices)
      distToPivot(i) = leftPoint.minDist(pivots(i))

    val thetaPq = new BoundedPriorityQueue[Double](k)(new DescendOrdering)
    rightMetaInfo.foreach(right => {
      if (rightDataByPivot(right.partitionId) != null){
        right.knnOfPivot.foreach {d =>
          val tempDist = distToPivot(right.partitionId) + d
          thetaPq += tempDist
        }
      }
    })
    var theta = thetaPq.head

    var knnOfLeftItem = new BoundedPriorityQueue[(Row, Double)](k)(new DescendOrderRowDistance)
    val outer_loop = new Breaks
    outer_loop.breakable {
      sortByDist.foreach(cur_part => {
        if (cur_part._2 == Double.MaxValue)
          outer_loop.break()

        val leftObjectToHP = if (cur_part._1 == leftPid) 0.0
          else (distToPivot(cur_part._1) * distToPivot(cur_part._1) - distToPivot(leftPid) * distToPivot(leftPid)) /
                (2.0 * pivots(leftPid).minDist(pivots(cur_part._1)))

        if (leftObjectToHP <= theta) {
          val loop = new Breaks
          loop.breakable{
            for (i <- rightDataByPivot(cur_part._1).indices) {
              val rightItem = rightDataByPivot(cur_part._1)(i)
              if (distToPivot(cur_part._1) > rightItem._3 + theta)
                loop.break()

              knnOfLeftItem += ((rightItem._2, rightItem._1.minDist(leftPoint)))
              if (knnOfLeftItem.size == k) theta = knnOfLeftItem.head._2
            }
          }
        }
      })
    }
    knnOfLeftItem.map(item => new JoinedRow(leftRow, item._1))
  }


  def knnObjectForEachPartition(leftDataByPivot: Array[Array[(Point, Row)]],
                                rightDataByPivot: Array[Array[(Point, Row, Double)]],
                                pivots: Array[Point],
                                rightMetaInfo: Array[RightMetaInfo]): ListBuffer[Row] ={
    var res: ListBuffer[Row] = ListBuffer[Row]()

    for (i <- rightDataByPivot.indices)
      if (rightDataByPivot(i) != null)
        rightDataByPivot(i) = rightDataByPivot(i).sortWith((x, y) => x._3 > y._3)

    for (left_pid <- leftDataByPivot.indices) { // foreach left partition
      if (leftDataByPivot(left_pid) != null) {
        val leftData = leftDataByPivot(left_pid)
        val sortByDist = (0 to num_of_pivots - 1).map(x => {
          if (rightDataByPivot(x) == null) (x, Double.MaxValue)
          else (x, pivots(left_pid).minDist(pivots(x)))
        }).sortWith(_._2 < _._2)

        leftData.foreach(leftItem =>
          res ++= knnObjectForSinglePoint(leftItem, left_pid, sortByDist, pivots, rightMetaInfo, rightDataByPivot)
        )
      }
    }
    res
  }

  class DescendOrdering extends Ordering[Double] {
    override def compare(x: Double, y: Double): Int = -x.compare(y)
  }

  class DescendOrderRowDistance extends Ordering[(Row, Double)] {
    def compare(a: (Row, Double), b: (Row, Double)): Int = {
      - a._2.compare(b._2)
    }
  }

  override def execute() = {
    val leftRDD = left.execute().map(row => {
      val tmp : Array[Double] = new Array[Double](dimension)
      for (i <-0 to dimension - 1)
        tmp(i) = BindReferences.bindReference(leftKeys(i), left.output).eval(row).asInstanceOf[Number].doubleValue()
      (Point(tmp, dimension), row)
    })
    val rightRDD = right.execute().map(row => {
      val tmp : Array[Double] = new Array[Double](dimension)
      for (i <-0 to dimension - 1)
        tmp(i) = BindReferences.bindReference(rightKeys(i), right.output).eval(row).asInstanceOf[Number].doubleValue()
      (Point(tmp, dimension), row)
    })

    val pivots = generatePivots(leftRDD.map(_._1).union(rightRDD.map(_._1)), num_of_pivots)
    val bc_pivots = sparkContext.broadcast(pivots)
    val leftWithPivots = leftRDD.mapPartitions(iter => iter.map(x => {
      var nearestDist = Double.MaxValue
      var ans = -1
      val point = x._1
      val local_pivots = bc_pivots.value
      for (i <- local_pivots.indices){
        val dist = point.minDist(local_pivots(i))
        if (dist < nearestDist) {
          nearestDist = dist
          ans = i
        }
      }
      (ans, x)
    }))
    val rightWithPivots = rightRDD.mapPartitions(iter => iter.map(x => {
      var nearestDist = Double.MaxValue
      var ans = -1
      val point = x._1
      val local_pivots = bc_pivots.value
      for (i <- local_pivots.indices){
        val dist = point.minDist(local_pivots(i))
        if (dist < nearestDist) {
          nearestDist = dist
          ans = i
        }
      }
      (ans, x)
    }))

    // calculate the number of records in every partition of the left table
    val cell_size = leftWithPivots.aggregate(Array.fill[Int](num_of_pivots)(0))((tmp, now) => {
      tmp(now._1) += 1
      tmp
    }, (left, right) => {
      left.zip(right).map(x => x._1 + x._2)
    })

    val (grouping, pivot_to_group) = geoGrouping(pivots, cell_size, num_partitions)

    val leftPartitioned = voronoiPartition(leftWithPivots, pivot_to_group, num_partitions)
    val rightPartitioned = voronoiPartition(rightWithPivots, pivot_to_group, num_partitions)

    val tmp_leftMetaInfo = leftPartitioned.mapPartitions(iter => {
      val data = iter.toArray
      data.groupBy(_._1).map(x => {
        val pivot = bc_pivots.value(x._1)
        var upperBound = Double.MinValue
        x._2.foreach(p => {
          val dist = p._2._1.minDist(pivot)
          if (dist > upperBound) upperBound = dist
        })
        LeftMetaInfo(x._1, upperBound)
      }).iterator
    }).collect()

    val rightMetaInfo = rightPartitioned.mapPartitions(iter => {
      val data = iter.toArray
      data.groupBy(_._1).map(x => {
        var upperBound = Double.MinValue
        val pivot = bc_pivots.value(x._1)
        val minDist = new BoundedPriorityQueue[Double](k)(new DescendOrdering)
        x._2.foreach(p => {
          val dist = p._2._1.minDist(pivot)
          if (dist > upperBound) upperBound = dist
          minDist += dist
        })
        RightMetaInfo(x._1, upperBound, minDist.toArray)
      }).iterator
    }).collect()

    val leftMetaInfo = new mutable.HashMap[Int, LeftMetaInfo]
    tmp_leftMetaInfo.foreach(left => {
      val thetaArray = new BoundedPriorityQueue[Double](k)(new DescendOrdering)
      rightMetaInfo.foreach(right => {
        right.knnOfPivot.foreach {d =>
          val leftPivot = pivots(left.partitionId)
          val rightPivot = pivots(right.partitionId)
          val tempDist = left.upperBound + leftPivot.minDist(rightPivot) + d
          thetaArray += tempDist
        }
      })
      left.theta = thetaArray.head
      leftMetaInfo(left.partitionId) = left
    })

    class PartitionOrdering extends Ordering[LeftMetaInfo] {
      override def compare(x: LeftMetaInfo, y: LeftMetaInfo): Int = x.partitionId.compare(y.partitionId)
    }

    val lowerBound: Array[Array[Double]] = new Array(num_partitions)

    for (i <- grouping.indices) {
      lowerBound(i) = new Array[Double](num_of_pivots)
      rightMetaInfo.foreach(right => {
        var minLB = right.upperBound + 1
        grouping(i).foreach(x => {
          if (leftMetaInfo.get(x).isDefined) {
            val left = leftMetaInfo(x)
            val lb = pivots(left.partitionId).minDist(pivots(right.partitionId)) - left.upperBound - left.theta
            if (lb < minLB)
              minLB = lb
          }
        })
        lowerBound(i)(right.partitionId) = minLB
      })
    }

    val rightDuplicated = rightPartitioned.mapPartitions{iter => {
      var ans = ListBuffer[(Int, ((Point, Int), Row))]()
      while (iter.hasNext) {
        val now = iter.next()
        for (left <- 0 to num_partitions - 1)
          if (now._2._1.minDist(bc_pivots.value(now._1)) >= lowerBound(left)(now._1))
            ans += ((left, ((now._2._1, now._1), now._2._2)))
      }
      ans.toArray.iterator
    }}

    val rightDupPartitioned = mapDPartition(rightDuplicated, num_partitions)

    leftPartitioned.zipPartitions(rightDupPartitioned) {
      (leftIter, rightIter) => {
        var ans = ListBuffer[Row]()
        if (rightIter.hasNext) {
          val leftDataByPivot = new Array[Array[(Point, Row)]](num_of_pivots)

          leftIter.foreach(x => {
            if (leftDataByPivot(x._1) == null) leftDataByPivot(x._1) = Array(x._2)
            else leftDataByPivot(x._1) = leftDataByPivot(x._1) :+ x._2
          })

          val rightDataByPivot = new Array[Array[(Point, Row, Double)]](num_of_pivots)

          rightIter.foreach(x => { // x._1: where to shuffle, x._2._1._2: the partitionId of right element
            if (rightDataByPivot(x._2._1._2) == null)
              rightDataByPivot(x._2._1._2) = Array((x._2._1._1, x._2._2,
                bc_pivots.value(x._2._1._2).minDist(x._2._1._1)))
            else rightDataByPivot(x._2._1._2) = rightDataByPivot(x._2._1._2) :+
              (x._2._1._1, x._2._2, bc_pivots.value(x._2._1._2).minDist(x._2._1._1))
          })
          ans ++= knnObjectForEachPartition(leftDataByPivot, rightDataByPivot, bc_pivots.value, rightMetaInfo)
        }
        ans.iterator
      }
    }
  }
}