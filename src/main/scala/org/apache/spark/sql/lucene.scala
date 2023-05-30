package org.apache.spark.sql

object lucene {
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
