package org.seabow.spark.v2.lucene.collector

import org.apache.lucene.index.ReaderUtil
import org.apache.lucene.queries.function.valuesource.{BytesRefFieldSource, FieldCacheSource}
import org.apache.lucene.search.{IndexSearcher, ScoreMode, SimpleCollector}
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class RowCollector(searcher:IndexSearcher, fields:Array[String]) extends SimpleCollector {
  var totalHits = 0
  var docs=ArrayBuffer[Int]()
  var resultData: ArrayBuffer[Row] = ArrayBuffer()
  private var fieldCacheMap: Map[String, FieldCacheSource] = {
    fields.map(f =>
      (f, new BytesRefFieldSource(f))
    ).toMap
  }
  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES


  override def collect(doc: Int): Unit = {
    totalHits += 1
    docs.append(doc)
    //use doc value first or use searcher.doc() directly?
    //情况分析
    //1)所有fields都有docvalue
    //1. field数量大>5的情况下用searcher.doc
    // 2.field数量<5的情况下用FieldCacheSource
      if (fields!=null && fields.size < 5 && fields.size > 0) {
        val leaves = searcher.getTopReaderContext().leaves()
        val context = leaves.get(ReaderUtil.subIndex(doc, leaves))
        val values= fields.map { f => fieldCacheMap(f).getValues(null, context).strVal(doc)
        }
        resultData.append(Row.apply(values:_*))
      } else {
        val document=searcher.doc(doc,fields.toSet.asJava)
        val values=fields.map {
          field =>
            document.get(field)
        }
        resultData.append(Row.apply(values:_*))
      }

  }
}
