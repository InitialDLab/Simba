package org.apache.spark.sql.index

/**
 * Created by dong on 1/15/16.
 * Base Traits for index, and definition of index type
 */
trait Index

object IndexType {
  def apply(ty: String): IndexType = ty.toLowerCase match {
    case "rtree" => RTreeType
    case "treemap" => TreeMapType
    case "hashmap" => HashMapType
    case _ => null
  }
}

sealed abstract class IndexType

case object RTreeType extends IndexType
case object TreeMapType extends IndexType
case object HashMapType extends IndexType
