package org.apache.spark.sql.index

/**
 * Created by Dong Xie on 9/22/15.
 * Base trait for index and index type
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