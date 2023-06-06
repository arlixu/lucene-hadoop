package org.apache.spark.sql.v2.lucene.util

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.seabow.spark.v2.lucene.LuceneOptions

object LuceneUtils {
  def inferSchema(sparkSession: SparkSession, files: Seq[FileStatus], options: Map[String, String])
  : Option[StructType] = {
    val file = files.head
    val conf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val luceneOptions=new LuceneOptions(options,sparkSession.sessionState.conf)
    val fs = file.getPath.getFileSystem(conf)
    val schemaPath = new Path(file.getPath.toString + ".dir/.schema")
    var ddl = ""
    if (fs.exists(schemaPath)) {
      var result = new Array[Byte](0)
      val inputStream = fs.open(schemaPath)
      val stat = fs.getFileStatus(schemaPath)
      val length = stat.getLen.toInt
      val buffer = new Array[Byte](length)
      inputStream.readFully(buffer)
      inputStream.close()
      result = buffer
      ddl = result.map(_.toChar).mkString
    }
    if (!ddl.isEmpty) {
      val schemaFromDDL=StructType.fromDDL(ddl)
      if(luceneOptions.enforceFacetSchema){
        Some(arrayToAtomicType(schemaFromDDL).asInstanceOf[StructType])
      }else{
        Some(schemaFromDDL)
      }
    } else {
      None
    }
  }

  //支持schema字段类型简化。
  def arrayToAtomicType(dataType: DataType): DataType = dataType match {
    case arrayType: ArrayType if arrayType.elementType.isInstanceOf[AtomicType] =>
      arrayType.elementType.asInstanceOf[AtomicType]
    case mapType @ MapType(keyType,valueType:ArrayType,_) if valueType.elementType.isInstanceOf[AtomicType] =>
      mapType.copy(keyType,valueType.elementType.asInstanceOf[AtomicType])
    case structType: StructType =>
      val fields = structType.fields.map(field => field.copy(dataType = arrayToAtomicType(field.dataType)))
      StructType(fields)
    case otherType => otherType
  }

}
