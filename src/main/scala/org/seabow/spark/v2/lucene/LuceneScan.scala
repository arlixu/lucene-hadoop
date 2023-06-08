package org.seabow.spark.v2.lucene
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SparkSession, SparkUtils}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v3.evolving.expressions.aggregate.Aggregation
import org.apache.spark.util.SerializableConfiguration
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
                       var buildByHolder:Boolean=false) extends FileScan{

  def getExecutorLocations():Seq[String]={
    val locations=SparkUtils.getExecutorLocations(sparkSession.sparkContext)
    locations
  }

  def hashFunction(path: String, numExecutors: Int): Int = {
    val hashCode = path.hashCode() & Integer.MAX_VALUE // 使用路径的哈希码
    hashCode % numExecutors // 取模以得到执行器索引
  }
  var executorLocations=getExecutorLocations()

  override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    LucenePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters,pushedAggregate)
  }

  lazy private val (pushedAggregationsStr, pushedGroupByStr) = if (pushedAggregate.nonEmpty) {
    (seqToString(pushedAggregate.get.aggregateExpressions),
      seqToString(pushedAggregate.get.groupByExpressions))
  } else {
    ("[]", "[]")
  }
  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)+",PushedAggregation:"+pushedAggregationsStr+",PushedGroupBy:"+pushedGroupByStr
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val pp=partitions.map{
      p:FilePartition=>
       val locatedFiles= p.files.map{
          pf=>
            val locations=if (executorLocations.size>0){Array(executorLocations(hashFunction(pf.filePath,executorLocations.size)))} else Array.empty[String]
          PartitionedFile(pf.partitionValues,pf.filePath,pf.start,pf.length,locations)
        }
        FilePartition(p.index,locatedFiles)
    }.toArray
    pp.toSeq.toArray
  }

}