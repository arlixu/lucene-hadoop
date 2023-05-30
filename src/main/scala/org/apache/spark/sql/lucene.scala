package org.apache.spark.sql

object lucene {
  val LUCENE_FILE_SUFFIX = ".lucene"
  val LUCENE_DIR_SUFFIX = ".lucene.dir"
  implicit class LuceneWriter[T](dataFrameWriter: DataFrameWriter[T]) {
    def lucene(path: String): Unit = {
      dataFrameWriter.format("lucene").save(path)
    }
  }

  implicit class LuceneReader(dataFrameReader: DataFrameReader) {
    def lucene(path: String): Unit = {
      dataFrameReader.format("lucene").load(path)
    }
  }
}
