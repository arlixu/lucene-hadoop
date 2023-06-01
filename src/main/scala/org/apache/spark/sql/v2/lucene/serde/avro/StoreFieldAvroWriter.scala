package org.apache.spark.sql.v2.lucene.serde.avro

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.spark.sql.avro.{AvroSerializer, SchemaConverters}
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}

import java.io.ByteArrayOutputStream

case class StoreFieldAvroWriter(schema:StructType){

  def dataType: DataType = BinaryType

  @transient private lazy val avroType =SchemaConverters.toAvroType(schema, false)

  @transient private lazy val serializer =
    new AvroSerializer(schema, avroType, true)

  @transient private lazy val writer =
    new GenericDatumWriter[Any](avroType)

  @transient private var encoder: BinaryEncoder = _

  @transient private lazy val out = new ByteArrayOutputStream

  def getAndReset(input: Any): Array[Byte] = {
    out.reset()
    encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
    val avroData = serializer.serialize(input)
    writer.write(avroData, encoder)
    encoder.flush()
    out.toByteArray
  }
}
