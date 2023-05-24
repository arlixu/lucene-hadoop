package org.apache.spark.sql.v2.lucene

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.seabow.spark.v2.lucene.{LuceneScanBuilder, LuceneUtils}

case class LuceneTable(
                        name: String,
                        sparkSession: SparkSession,
                        options: CaseInsensitiveStringMap,
                        paths: Seq[String],
                        userSpecifiedSchema: Option[StructType],
                        fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    LuceneUtils.inferSchema(sparkSession, files, options.asScala.toMap)

  override def formatName: String = "lucene"

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new LuceneScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  def newScanBuilder(options: CaseInsensitiveStringMap,buildByHolder: Boolean): ScanBuilder =
    new LuceneScanBuilder(sparkSession, fileIndex, schema, dataSchema, options,buildByHolder)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = null

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap) // This is a non-streaming file based datasource.
    val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(paths, hadoopConf,
      checkEmptyGlobPath = true, checkFilesExist = true,globPaths)
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new InMemoryDirIndex(
      sparkSession, rootPathsSpecified, caseSensitiveMap, userSpecifiedSchema, fileStatusCache)

  }

  private def globPaths: Boolean = {
    val entry = options.get(DataSource.GLOB_PATHS_KEY)
    Option(entry).map(_ == "true").getOrElse(true)
  }
}
