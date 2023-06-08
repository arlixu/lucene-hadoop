package org.apache.spark.cache

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.seabow.HdfsDirectoryFactory

import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.Map

object LuceneSearcherCache extends Logging {
  val cachedSearcherMap: Map[Path, IndexSearcher] = Map[Path, IndexSearcher]().empty
  val luceneCacheAccumulator: LuceneCacheAccumulator = new LuceneCacheAccumulator

  def getSearcherInstance(filePath: String, conf: Configuration): IndexSearcher = {
    val luceneDirPath=new Path(new URI(filePath+".dir"))
    if (cachedSearcherMap.contains(luceneDirPath)) {
      log.info(s"get cached luceneDirPath searcher:${luceneDirPath.toString}")
      return cachedSearcherMap(luceneDirPath)
    } else {
      log.info(s"can't get cached luceneDirPath searcher,create a new searcher:${luceneDirPath.toString}")
      val reader = DirectoryReader.open(new HdfsDirectoryFactory().create(luceneDirPath.toString, conf))
      val searcher = new IndexSearcher(reader)
      cachedSearcherMap.put(luceneDirPath, searcher)
      val taskContext = TaskContext.get()
      val executorId = taskContext.getLocalProperty("spark.executor.id")
      val host = taskContext.getLocalProperty("spark.executor.host")
      val location = ExecutorCacheTaskLocation(host, executorId)
      println(s"location:$location")
      LuceneSearcherCache.luceneCacheAccumulator.add(Map(filePath-> mutable.Set(location.toString())))
      return searcher
    }
  }
}
