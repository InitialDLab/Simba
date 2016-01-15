package org.apache.spark.sql.index

/**
 * Created by crystalove on 15-5-27.
 */
trait Index

object IndexType {
    def apply(typ : String) : IndexType = typ.toLowerCase match {
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