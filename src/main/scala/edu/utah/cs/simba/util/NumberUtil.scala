package edu.utah.cs.simba.util

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types._

/**
  * Created by dongx on 11/7/2016.
  */
object NumberUtil {
  def literalToDouble(x: Literal): Double = {
    x.value match {
      case double_value: Number =>
        double_value.doubleValue()
      case decimal_value: Decimal =>
        decimal_value.toDouble
    }
  }

  def isIntegral(x: DataType): Boolean = {
    x match {
      case IntegerType => true
      case LongType => true
      case ShortType => true
      case ByteType => true
      case _ => false
    }
  }
}
