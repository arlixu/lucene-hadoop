package org.seabow.spark.v2.lucene.collector

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.{ScoreMode, SimpleCollector}

import scala.collection.mutable.ListBuffer

class PagingCollector (page: Int, resultsPerPage: Int) extends SimpleCollector{
  private var currentPage: Int = page
  private var collectedResults: Int = 0
  var docs=ListBuffer[Int]()
  private var docBase: Int = 0
  var hasNextPage=false

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    docBase = context.docBase
  }

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
  override def collect(doc: Int): Unit = {
    // 检查是否已经收集足够的结果，如果是则停止收集
    if (collectedResults >= currentPage * resultsPerPage) {
      hasNextPage=true
      return
    }

    // 检查是否达到当前页的起始位置，如果是则开始收集结果
    if (collectedResults >= (currentPage - 1) * resultsPerPage) {
      // 处理结果
      docs.append(docBase+doc)
    }

    collectedResults += 1
  }

}
