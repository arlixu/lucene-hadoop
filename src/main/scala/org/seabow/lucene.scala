package org.seabow

import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}

object lucene {
  implicit class LuceneWriter[T](dataFrameWriter: DataFrameWriter[T]){
    def lucene(path: String): Unit = {
      dataFrameWriter.format("lucene").save(path)
    }
  }
  implicit class LuceneReader(dataFrameReader:DataFrameReader){
    def lucene(path: String): Unit = {
      dataFrameReader.format("lucene").load(path)
    }
  }
}
