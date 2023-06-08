package org.apache.spark.sql.v2.lucene

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus}
import org.apache.spark.cache.LuceneSearcherCache
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v3.evolving.expressions.aggregate.Aggregation
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import org.seabow.spark.v2.lucene.LucenePartitionReaderFactory

import java.util.Locale

case class LuceneScan(
                       sparkSession: SparkSession,
                       hadoopConf: Configuration,
                       fileIndex: PartitioningAwareFileIndex,
                       dataSchema: StructType,
                       readDataSchema: StructType,
                       readPartitionSchema: StructType,
                       options: CaseInsensitiveStringMap,
                       var pushedFilters: Array[Filter],
                       pushedAggregate: Option[Aggregation] = None,
                       partitionFilters: Seq[Expression] = Seq.empty,
                       dataFilters: Seq[Expression] = Seq.empty,
                       var buildByHolder: Boolean = false) extends FileScan {
  val luceneCacheAccumulator = LuceneSearcherCache.registerLuceneCacheAccumulatorInstances(sparkSession)
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis


  override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    LucenePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters, pushedAggregate, luceneCacheAccumulator)
  }

  lazy private val (pushedAggregationsStr, pushedGroupByStr) = if (pushedAggregate.nonEmpty) {
    (seqToString(pushedAggregate.get.aggregateExpressions),
      seqToString(pushedAggregate.get.groupByExpressions))
  } else {
    ("[]", "[]")
  }

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters) + ",PushedAggregation:" + pushedAggregationsStr + ",PushedGroupBy:" + pushedGroupByStr
  }

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations.filter(_.getLength>4096)
    case f => Array.empty[BlockLocation]
  }

  private def getBlockHosts(blockLocations:Array[BlockLocation]): Array[String] ={
    val sortedLocations = blockLocations.sortBy(_.getLength)(Ordering.Long.reverse)
    val topThreeLocations = sortedLocations.take(3).map(_.getHosts)
    val blockHosts = topThreeLocations.flatten
    blockHosts
  }

  override def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val partitionAttributes = fileIndex.partitionSchema.toAttributes
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.get(normalizeName(readField.name)).getOrElse {
        throw new AnalysisException(s"Can't find required partition column ${readField.name} " +
          s"in partition schema ${fileIndex.partitionSchema}")
      }
    }
    lazy val partitionValueProject = {
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    }
    val seperateFiles = selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }
      partition.files.flatMap { file =>
        val filePath = file.getPath
       Array(PartitionedFile(partitionValues, filePath.toUri.toString, 0, file.getLen,getBlockHosts(getBlockLocations(file))))
      }
    }
   val partitions= for (i<- 0 until seperateFiles.size) yield {
      FilePartition(i,Array(seperateFiles(i)))
    }
    partitions
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val executorCacheLocations = luceneCacheAccumulator.value
    val pp = partitions.map { p: FilePartition =>
      val locatedFiles = p.files.map { pf =>
        // 1. 尝试将文件分配到缓存节点
        val cacheLocations = executorCacheLocations.getOrElse(pf.filePath, Set.empty[String])
        val partitionedFile =
          if (cacheLocations.nonEmpty) {
            PartitionedFile(pf.partitionValues, pf.filePath, pf.start, pf.length, cacheLocations.toArray++pf.locations)
          } else {
            // 2. 如果没有缓存节点，则分配到块位置
            pf
          }
        partitionedFile
      }
     new DirPartition(p.index, locatedFiles)
    }.toArray
    pp.toSeq.toArray
  }

}

class DirPartition(override val index: Int, override val  files: Array[PartitionedFile])
  extends FilePartition(index, files) {
  override def preferredLocations(): Array[String] = {
   files.head.locations
  }
}

