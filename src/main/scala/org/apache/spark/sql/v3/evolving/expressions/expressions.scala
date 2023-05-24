package org.apache.spark.sql.v3.evolving.expressions

import org.apache.spark.sql.connector.expressions.{FieldReference, Literal, LiteralValue, NamedReference}
import org.apache.spark.sql.types.{DataType, StringType}

case class V2FieldReference(fieldReference: FieldReference) extends V2Expression with NamedReference{
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
  override def children(): Array[V2Expression] = new Array[V2Expression](0);
  override def fieldNames: Array[String] = fieldReference.parts.toArray
  override def describe: String = fieldReference.parts.quoted
  override def toString: String = describe
}

case class V2LiteralValue[T](literalValue:LiteralValue[T]) extends Literal[T] with V2Expression{
  override def describe: String = {
    if (literalValue.dataType.isInstanceOf[StringType]) {
      s"'$value'"
    } else {
      s"$value"
    }
  }
  override def toString: String = describe

  override def value(): T = literalValue.value

  override def dataType(): DataType = literalValue.dataType

  /**
   * Returns an array of the children of this node. Children should not change.
   */
  override def children(): Array[V2Expression] = new Array[V2Expression](0);
}