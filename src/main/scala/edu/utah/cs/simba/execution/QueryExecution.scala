package edu.utah.cs.simba.execution

import edu.utah.cs.simba.SimbaContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, QueryExecution => SQLQueryExecution}

/**
  * Created by dongx on 11/12/2016.
  */
class QueryExecution(val simbaContext: SimbaContext, val simbaLogical: LogicalPlan)
  extends SQLQueryExecution(simbaContext, simbaLogical) {
  lazy val withIndexedData: LogicalPlan = {
    assertAnalyzed()
    simbaContext.indexManager.useIndexedData(withCachedData)
  }

  override lazy val optimizedPlan: LogicalPlan = {
    simbaContext.simbaOptimizer.execute(simbaContext.getSQLOptimizer.execute(withIndexedData))
  }

  override lazy val sparkPlan: SparkPlan = {
    SQLContext.setActive(sqlContext)
    SimbaContext.setActive(simbaContext)
    simbaContext.simbaPlanner.plan(optimizedPlan).next()
  }
}
