package org.seabow.spark.v2.lucene

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.lucene

class LuceneDataSource extends FileDataSourceV2{
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[LuceneFileFormat]


  override def shortName(): String = "lucene"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths=getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    lucene.LuceneTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }


  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    lucene.LuceneTable(tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }

}
