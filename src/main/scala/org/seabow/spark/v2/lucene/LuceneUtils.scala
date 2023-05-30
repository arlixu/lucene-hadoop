package org.seabow.spark.v2.lucene

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object LuceneUtils {
  def inferSchema(sparkSession: SparkSession, files: Seq[FileStatus], options: Map[String, String])
  : Option[StructType] = {
    val file=files.head
    val conf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs=file.getPath.getFileSystem(conf)
    val schemaPath=new Path(file.getPath.toString+".dir/.schema")
    var ddl=""
    if (fs.exists(schemaPath)) {
      var result = new Array[Byte](0)
      val inputStream = fs.open(schemaPath)
      val stat = fs.getFileStatus(schemaPath)
      val length = stat.getLen.toInt
      val buffer = new Array[Byte](length)
      inputStream.readFully(buffer)
      inputStream.close()
      result = buffer
       ddl= result.map(_.toChar).mkString
    }
    if(!ddl.isEmpty){
      Some(StructType.fromDDL(ddl))
    }else{
      None
    }
  }

}
