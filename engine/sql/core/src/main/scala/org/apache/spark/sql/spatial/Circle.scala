/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.spatial

/**
  * Created by dong on 3/16/16.
  */
case class Circle(center: Point, radius: Double) extends Shape {
  override def minDist(other: Shape): Double = {
    other match {
      case p @ Point(_) => minDist(p)
      case mbr @ MBR(_, _) => minDist(mbr)
      case cir @ Circle(_, _) => minDist(cir)
    }
  }

  def minDist(other: Point): Double = {
    require(center.coord.length == other.coord.length)
    if (contains(other)) 0.0
    else other.minDist(center) - radius
  }

  def minDist(other: MBR): Double = {
    require(center.coord.length == other.low.coord.length)
    if (isIntersect(other)) 0.0
    else center.minDist(other) - radius
  }

  def minDist(other: Circle): Double = {
    require(center.coord.length == other.center.coord.length)
    if (isIntersect(other)) 0.0
    else center.minDist(other.center) - radius - other.radius
  }


  override def isIntersect(other: Shape): Boolean = {
    other match {
      case p @ Point(_) => contains(p)
      case mbr @ MBR(_, _) => isIntersect(mbr)
      case cir @ Circle(_, _) => isIntersect(cir)
    }
  }

  def contains(p: Point): Boolean = p.minDist(center) <= radius

  def isIntersect(other: MBR): Boolean = center.minDist(other) <= radius

  def isIntersect(other: Circle): Boolean = other.center.minDist(center) <= other.radius + radius

  def getMBR: MBR = new MBR(center.shift(-radius), center.shift(radius))

  override def toString: String = "CIRCLE(" + center.toString + "," + radius + ")"
}
