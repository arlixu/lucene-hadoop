package org.apache.spark.sql.v2.lucene.serde

import org.apache.lucene.document.Document
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JacksonParser}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class LuceneDeserializer(dataSchema: StructType,
                         requiredSchema: StructType,zoneId: String) {

  private val resultRow = new SpecificInternalRow(requiredSchema.map(_.dataType))
  val nameOfCorruptRecord = SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)
  val parsedOptions = new JSONOptions(Map.empty, zoneId)

  val parsers=requiredSchema.map{structField=>
    structField.dataType match {
      case ArrayType(elementType, _)=>
        Some(new JacksonParser(structField.dataType, parsedOptions, allowArrayAsStructs = false))
      case MapType(keyType, valueType, _)=>
        Some(new JacksonParser( structField.dataType, parsedOptions, allowArrayAsStructs = false))
      case StructType(fields)=>
        Some(new JacksonParser( structField.dataType, parsedOptions, allowArrayAsStructs = false))
      case _=>None
    }
  }
  val createParser = CreateJacksonParser.utf8String _

  def deserialize(doc: Document): InternalRow = {
    val length=requiredSchema.length
    for(idx<- 0 until(length)){
      val value=doc.get(requiredSchema(idx).name)
      if(value==null){
        resultRow.setNullAt(idx)
      }else{
        requiredSchema(idx).dataType match {
          case IntegerType =>
            resultRow.setInt(idx,doc.get(requiredSchema(idx).name).toInt)
          case LongType =>
            resultRow.setLong(idx,doc.get(requiredSchema(idx).name).toLong)
          case FloatType =>
            resultRow.setFloat(idx,doc.get(requiredSchema(idx).name).toFloat)
          case DoubleType =>
            resultRow.setDouble(idx,doc.get(requiredSchema(idx).name).toDouble)
          case StringType =>
            resultRow.update(idx, UTF8String.fromBytes(doc.get(requiredSchema(idx).name).getBytes))
          case ArrayType(elementType, _) =>
           val rows:Iterator[InternalRow]= parsers(idx).get.parse(UTF8String.fromBytes(doc.get(requiredSchema(idx).name).getBytes),createParser, identity[UTF8String]).toIterator
           val value= if (rows.hasNext) rows.next().getArray(0) else null
            resultRow.update(idx,value)
          case MapType(keyType, valueType, _)=>
            val rows:Iterator[InternalRow]= parsers(idx).get.parse(UTF8String.fromBytes(doc.get(requiredSchema(idx).name).getBytes),createParser, identity[UTF8String]).toIterator
            val value= if (rows.hasNext) rows.next().getMap(0) else null
            resultRow.update(idx,value)
          case StructType(fields)=>
            val rows:Iterator[InternalRow]= parsers(idx).get.parse(UTF8String.fromBytes(doc.get(requiredSchema(idx).name).getBytes),createParser, identity[UTF8String]).toIterator
            val value= if (rows.hasNext) rows.next() else null
            resultRow.update(idx,value)
          case _=>

        }
      }
    }
    resultRow
  }
}
