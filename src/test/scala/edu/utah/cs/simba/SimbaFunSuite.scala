package edu.utah.cs.simba

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Helper trait for SQL test suite
 */
trait SimbaFunSuite extends FunSuite with BeforeAndAfterAll {
  private var sparkConf: SparkConf = null
  private var sparkContext: SparkContext = null
  private var _ctx: SimbaContext = null
  def simbaContext: SimbaContext = _ctx

  protected override def beforeAll(): Unit = {
    if (sparkConf == null) {
      sparkConf = new SparkConf().setAppName("SharedSQLContext").setMaster("local")
    }
    if (sparkContext == null) {
      sparkContext = new SparkContext(sparkConf)
    }
    if (_ctx == null) {
      _ctx = new SimbaContext(sparkContext)
    }
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    try {
      if (sparkContext != null) {
        sparkContext.stop()
        sparkContext = null
        _ctx = null
        sparkConf = null
      }
    } finally {
      super.afterAll()
    }
  }
}
