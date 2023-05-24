package org.seabow.spark.v2.lucene.cache

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.internal.Logging
import org.seabow.HdfsDirectoryFactory

import scala.collection.mutable.Map

object LuceneSearcherCache extends Logging{
  val cachedSearcherMap: Map[Path, IndexSearcher] = Map[Path, IndexSearcher]().empty
  def getSearcherInstance(path: Path,conf:Configuration): IndexSearcher = {
    if (cachedSearcherMap.contains(path)) {
      log.info(s"get cached path searcher:${path.toString}")
      return cachedSearcherMap(path)
    } else {
      log.info(s"can't get cached path searcher,create a new searcher:${path.toString}")
      val reader = DirectoryReader.open(new HdfsDirectoryFactory().create(path.toString,conf))
      val searcher = new IndexSearcher(reader)
      cachedSearcherMap.put(path, searcher)
      return searcher
    }
  }
}
