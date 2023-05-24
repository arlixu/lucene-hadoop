package org.apache.spark.sql.v2.lucene.util

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources
import org.apache.spark.sql.v2.lucene.LuceneExpressionFilters
import org.apache.spark.sql.v3.evolving.SupportsLucenePushDownFilters

import scala.collection.mutable

object LucenePushDownUtils {
  def pushFilters(scanBuilder: ScanBuilder, filters: Seq[Expression])
  : (Seq[sources.Filter], Seq[Expression]) = {
    scanBuilder match {
      case r: SupportsLucenePushDownFilters =>
        // A map from translated data source leaf node filters to original catalyst filter
        // expressions. For a `And`/`Or` predicate, it is possible that the predicate is partially
        // pushed down. This map can be used to construct a catalyst filter expression from the
        // input filter, or a superset(partial push down filter) of the input filter.
        val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
        val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated =
            LuceneExpressionFilters.translateFilterWithMapping(filterExpr, Some(translatedFilterToExpr),
              nestedPredicatePushdownEnabled = true)
          if (translated.isEmpty) {
            untranslatableExprs += filterExpr
          } else {
            translatedFilters += translated.get
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilters.toArray).map { filter =>
          DataSourceStrategy.rebuildExpressionFromFilter(filter, translatedFilterToExpr)
        }
        // Normally translated filters (postScanFilters) are simple filters that can be evaluated
        // faster, while the untranslated filters are complicated filters that take more time to
        // evaluate, so we want to evaluate the postScanFilters filters first.
        (r.pushedFilters().toSeq, (postScanFilters ++ untranslatableExprs).toSeq)

      case _ => (Nil, filters)
    }
  }
}
