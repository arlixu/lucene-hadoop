package org.apache.spark.sql.v2.lucene

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode, ProjectExec, SparkPlan}
import org.seabow.spark.v2.lucene.LuceneScan

class LuceneStrategy extends Strategy{
  private def withProjectAndFilter(
                                    project: Seq[NamedExpression],
                                    filters: Seq[Expression],
                                    scan: LeafExecNode,
                                    needsUnsafeConversion: Boolean): SparkPlan = {
    val filterCondition = filters.reduceLeftOption(And)
    val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

    if (withFilter.output != project || needsUnsafeConversion) {
      ProjectExec(project, withFilter)
    } else {
      withFilter
    }
  }
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match{
    case PhysicalOperation(project, filters, relation @ DataSourceV2ScanRelation(table,scan:LuceneScan,_)) =>
             print("print filter")
             val (canPushDownFilters,exprFilters)=LuceneExpressionFilters.translateFilters(filters)
             scan.pushedFilters=scan.pushedFilters++canPushDownFilters
             val batchExec = BatchScanExec(relation.output, relation.scan)
             withProjectAndFilter(project, exprFilters, batchExec, !batchExec.supportsColumnar) :: Nil
    case _ => Nil
  }
}
