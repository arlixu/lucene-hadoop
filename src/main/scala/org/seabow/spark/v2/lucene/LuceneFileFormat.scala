
/*
 * Copyright 2022 Martin Mauch (@nightscape)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seabow.spark.v2.lucene

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.LuceneFilters
import org.apache.spark.sql.v2.lucene.serde.LuceneDeserializer
import org.apache.spark.util.SerializableConfiguration
import org.seabow.spark.v2.lucene.cache.LuceneSearcherCache
import org.seabow.spark.v2.lucene.collector.PagingCollector

import java.net.URI

/** derived from binary file data source. Needed to support writing Lucene using the V2 API
 */
class LuceneFileFormat extends FileFormat with DataSourceRegister {

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]
                          ): Option[StructType] = {
    LuceneUtils.inferSchema(sparkSession, files, options)
  }

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType
                           ): OutputWriterFactory = {
    val LuceneOptions = new LuceneOptions(options, sparkSession.conf.get("spark.sql.session.timeZone"))

    new OutputWriterFactory {
      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new LuceneOutputWriter(path, dataSchema, context, LuceneOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String =
        s".${LuceneOptions.fileExtension}"
    }
  }

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
    false
  }

  override def shortName(): String = "lucene"
  override def toString: String = "LUCENE"

  /*
  We need this class for writing only, thus reader is not implemented
   */
  override def buildReaderWithPartitionValues(
                                      sparkSession: SparkSession,
                                      dataSchema: StructType,
                                      partitionSchema: StructType,
                                      requiredSchema: StructType,
                                      filters: Seq[Filter],
                                      options: Map[String, String],
                                      hadoopConf: Configuration
                                    ): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value
      val filePath = new Path(new URI(file.filePath+".dir"))
      val searcher = LuceneSearcherCache.getSearcherInstance(filePath, conf)
      val query = LuceneFilters.createFilter(dataSchema, filters)
      val deserializer = new LuceneDeserializer(dataSchema, requiredSchema, SQLConf.get.getConf(SQLConf.SESSION_LOCAL_TIMEZONE))
      var currentPage = 1
      var pagingCollector = new PagingCollector(currentPage, Int.MaxValue)
      searcher.search(query, pagingCollector)
      var docIterator = pagingCollector.docs.iterator
      docIterator.map{doc =>deserializer.deserialize(searcher.doc(doc))}
    }
  }



}
