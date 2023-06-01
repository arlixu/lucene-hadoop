package org.apache.spark.sql.v2.lucene.serde

import org.apache.lucene.document.Document
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonParser}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.serde.avro.StoreFieldAvroReader

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
  val storeFieldAvroReader=StoreFieldAvroReader(dataSchema,requiredSchema)

  def deserialize(doc: Document): InternalRow = {
    storeFieldAvroReader.deserialize(doc.getBinaryValue("_source").bytes).asInstanceOf[InternalRow]
  }
}
