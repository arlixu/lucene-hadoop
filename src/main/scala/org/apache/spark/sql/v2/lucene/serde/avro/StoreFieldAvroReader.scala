package org.apache.spark.sql.v2.lucene.serde.avro

import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.StructType

import scala.util.control.NonFatal

case class StoreFieldAvroReader(writeDataType:StructType, readDataType:StructType){
  @transient private lazy val readSchema =
   SchemaConverters.toAvroType(readDataType, false)

  @transient private lazy val writeSchema =
     SchemaConverters.toAvroType(writeDataType, false)

  @transient private lazy val reader = new GenericDatumReader[Any](writeSchema,readSchema)

  @transient private lazy val deserializer = new AvroDeserializer(readSchema, readDataType)

  @transient private var decoder: BinaryDecoder = _

  @transient private var result: Any = _

  @transient private lazy val nullResultRow: Any = readDataType match {
    case st: StructType =>
      val resultRow = new SpecificInternalRow(st.map(_.dataType))
      for(i <- 0 until st.length) {
        resultRow.setNullAt(i)
      }
      resultRow

    case _ =>
      null
  }

  def deserialize(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    try {
      decoder = DecoderFactory.get().binaryDecoder(binary, 0, binary.length, decoder)
      result = reader.read(result, decoder)
      deserializer.deserialize(result)
    } catch {
      // There could be multiple possible exceptions here, e.g. java.io.IOException,
      // AvroRuntimeException, ArrayIndexOutOfBoundsException, etc.
      // To make it simple, catch all the exceptions here.
      case NonFatal(e) =>
        e.printStackTrace()
        nullResultRow
    }
  }
}
