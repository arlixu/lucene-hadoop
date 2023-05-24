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

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.v2.lucene.serde.LuceneGenerator

class LuceneOutputWriter(path: String, dataSchema: StructType, context: TaskAttemptContext, options:LuceneOptions)
  extends OutputWriter
    with Logging {

  private val gen = new LuceneGenerator(path, dataSchema, context.getConfiguration, options)
  gen.writeSchema()

  override def write(row: InternalRow): Unit = gen.write(row)

  override def close(): Unit = {
    log.warn("output closing")
    gen.close()}
}