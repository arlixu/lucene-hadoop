package org.seabow.spark.v2.lucene.collector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.v3.evolving.expressions.aggregate._

class FacetAggCollector(agg: Aggregation, aggSchema: StructType, dataSchema: StructType) extends AggCollector(agg, aggSchema, dataSchema) {

  private def explodeGroupByRowSet(ordinal: Int, docValues: Seq[Any], groupByRowSet: Set[InternalRow]): Set[InternalRow] = {
    if (docValues.nonEmpty) {
      val newGroupByRowSet = groupByRowSet.flatMap {
        row =>
          for (k <- 0 until docValues.length) yield {
            val copiedRow = row.copy()
            groupRowSetters(ordinal)(copiedRow, ordinal, docValues(k))
            copiedRow
          }
      }
      newGroupByRowSet
    } else {
      groupByRowSet
    }
  }

  override def collect(doc: Int): Unit = {
    val result = new SpecificInternalRow(aggSchema)
    var groupByRowSet = Set[InternalRow](result)
    if (!agg.groupByExpressions().isEmpty) {
      //group by agg
      for (i <- 0 until (groupBySchema.length)) {
        val docValues = getMultiDocValues(groupBySchema(i).name,groupBySchema(i).dataType, doc)
        groupByRowSet = explodeGroupByRowSet(i, docValues, groupByRowSet)
      }
    }
    val metricRows = groupByRowSet.toSeq.map {
      row =>
        if (!bucketMap.contains(row)) {
          val metricRow = new SpecificInternalRow(aggMetricSchema)
          bucketMap.put(row, metricRow)
          metricRow
        } else {
          bucketMap.get(row).get
        }
    }

    for (metricRow <- metricRows) {
      updateMetricRow(doc, metricRow)
    }
  }


}
