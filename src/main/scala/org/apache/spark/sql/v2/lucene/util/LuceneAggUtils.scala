package org.apache.spark.sql.v2.lucene.util

import org.apache.hadoop.conf.Configuration
import org.apache.lucene.search.{IndexSearcher, Query}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.v3.evolving.expressions.aggregate.Aggregation
import org.seabow.spark.v2.lucene.collector.{AggCollector, FacetAggCollector}

import scala.collection.mutable

object LuceneAggUtils {
    def createAggInternalRows(agg:Aggregation, searcher:IndexSearcher, query:Query,dataSchema: StructType,
                              readDataSchema: StructType,
                              partitionSchema: StructType,conf:Configuration):mutable.Iterable[InternalRow]={
      //A,两个collector，一个分组，一个聚合
      //B.一个collector，分组的同时做聚合。
      val enforceFacetSchema=conf.getBoolean("enforceFacetSchema",false)
      val collector=if(enforceFacetSchema){new FacetAggCollector(agg,readDataSchema,dataSchema)}else{
        new AggCollector(agg,readDataSchema,dataSchema)
      }
      searcher.search(query,collector)
      return collector.getResultRows()
    }
}
