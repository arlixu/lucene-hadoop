package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.connector.expressions.{FieldReference, Expression => V2Expression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.LuceneFilters
import org.apache.spark.sql.v3.evolving.expressions.V2FieldReference
import org.apache.spark.sql.v3.evolving.expressions.aggregate._
import org.apache.spark.sql.v3.evolving.util.V2ColumnUtils

/**
 * Utility class for aggregate push down to Parquet and ORC.
 */
object AggregatePushDownUtils {

  /**
   * Get the data schema for aggregate to be pushed down.
   */
  def getSchemaForPushedAggregation(
                                     aggregation: Aggregation,
                                     schema: StructType,
                                     partitionNames: Set[String]): Option[StructType] = {

    var finalSchema = new StructType()

    def getStructFieldForCol(colName: String): StructField = {
     val nameToDataType= LuceneFilters.nameToDataType(schema)
      StructField(colName, LuceneFilters.attrDataType(colName,nameToDataType))
    }

    def isPartitionCol(colName: String) = {
      partitionNames.contains(colName)
    }

    def processSum(agg:AggregateFunc):Boolean={
      val (columnName, aggType) = agg match {
        case sum: Sum if V2ColumnUtils.extractV2Column(sum.column).isDefined =>
          (V2ColumnUtils.extractV2Column(sum.column).get, "sum")
        case _ => return false
      }
      if (isPartitionCol(columnName)) {
        // don't push down partition column, footer doesn't have max/min for partition column
        return false
      }
      val structField = getStructFieldForCol(columnName)
      structField.dataType match {
        // not push down complex type
        // not push down Timestamp because INT96 sort order is undefined,
        // Parquet doesn't return statistics for INT96
        // not push down Parquet Binary because min/max could be truncated
        // (https://issues.apache.org/jira/browse/PARQUET-1685), Parquet Binary
        // could be Spark StringType, BinaryType or DecimalType.
        // not push down for ORC with same reason.
        case  FloatType | DoubleType  =>
          finalSchema = finalSchema.add(StructField(s"$aggType(" + structField.name + ")",DoubleType))
          true
        case  IntegerType | LongType  =>
          finalSchema = finalSchema.add(StructField(s"$aggType(" + structField.name + ")",LongType))
          true
        case _ =>
          false
      }
    }

    def processMinOrMax(agg: AggregateFunc): Boolean = {
      val (columnName, aggType) = agg match {
        case max: Max if V2ColumnUtils.extractV2Column(max.column).isDefined =>
          (V2ColumnUtils.extractV2Column(max.column).get, "max")
        case min: Min if V2ColumnUtils.extractV2Column(min.column).isDefined =>
          (V2ColumnUtils.extractV2Column(min.column).get, "min")
        case _ => return false
      }

      if (isPartitionCol(columnName)) {
        // don't push down partition column, footer doesn't have max/min for partition column
        return false
      }
      val structField = getStructFieldForCol(columnName)

      structField.dataType match {
        // not push down complex type
        // not push down Timestamp because INT96 sort order is undefined,
        // Parquet doesn't return statistics for INT96
        // not push down Parquet Binary because min/max could be truncated
        // (https://issues.apache.org/jira/browse/PARQUET-1685), Parquet Binary
        // could be Spark StringType, BinaryType or DecimalType.
        // not push down for ORC with same reason.
        case BooleanType | ByteType | ShortType | IntegerType
             | LongType | FloatType | DoubleType | DateType =>
          finalSchema = finalSchema.add(structField.copy(s"$aggType(" + structField.name + ")"))
          true
        case _ =>
          false
      }
    }

    aggregation.groupByExpressions.map(extractColName).foreach { colName =>
      finalSchema = finalSchema.add(getStructFieldForCol(colName.get))
    }

    aggregation.aggregateExpressions.foreach {
      case max: Max =>
        if (!processMinOrMax(max)) return None
      case min: Min =>
        if (!processMinOrMax(min)) return None
      case count: Count
        if V2ColumnUtils.extractV2Column(count.column).isDefined && !count.isDistinct =>
        val columnName = V2ColumnUtils.extractV2Column(count.column).get
        finalSchema = finalSchema.add(StructField(s"count($columnName)", LongType))
      case _: CountStar =>
        finalSchema = finalSchema.add(StructField("count(*)", LongType))
      case sum: Sum =>
        if (!processSum(sum)) return None
      case _ =>
        return None
    }

    Some(finalSchema)
  }

  /**
   * Check if two Aggregation `a` and `b` is equal or not.
   */
  def equivalentAggregations(a: Aggregation, b: Aggregation): Boolean = {
    a.aggregateExpressions.sortBy(_.hashCode())
      .sameElements(b.aggregateExpressions.sortBy(_.hashCode())) &&
      a.groupByExpressions.sortBy(_.hashCode())
        .sameElements(b.groupByExpressions.sortBy(_.hashCode()))
  }

  /**
   * Return the schema for aggregates only (exclude group by columns)
   */
  def getSchemaWithoutGroupingExpression(
                                          aggSchema: StructType,
                                          aggregation: Aggregation): StructType = {
    val numOfGroupByColumns = aggregation.groupByExpressions.length
    if (numOfGroupByColumns > 0) {
      new StructType(aggSchema.fields.drop(numOfGroupByColumns))
    } else {
      aggSchema
    }
  }

  def getSchemaWithoutAggregateExpression(
                                          aggSchema: StructType,
                                          aggregation: Aggregation): StructType = {
    val numOfAggColumns = aggregation.aggregateExpressions().length
      new StructType(aggSchema.fields.dropRight(numOfAggColumns))
  }

  /**
   * Reorder partition cols if they are not in the same order as group by columns
   */
  def reOrderPartitionCol(
                           partitionSchema: StructType,
                           aggregation: Aggregation,
                           partitionValues: InternalRow): InternalRow = {
    val groupByColNames = aggregation.groupByExpressions.flatMap(extractColName)
    assert(groupByColNames.length == partitionSchema.length &&
      groupByColNames.length == partitionValues.numFields, "The number of group by columns " +
      s"${groupByColNames.length} should be the same as partition schema length " +
      s"${partitionSchema.length} and the number of fields ${partitionValues.numFields} " +
      s"in partitionValues")
    var reorderedPartColValues = Array.empty[Any]
    if (!partitionSchema.names.sameElements(groupByColNames)) {
      groupByColNames.foreach { col =>
        val index = partitionSchema.names.indexOf(col)
        val v = partitionValues.asInstanceOf[GenericInternalRow].values(index)
        reorderedPartColValues = reorderedPartColValues :+ v
      }
      new GenericInternalRow(reorderedPartColValues)
    } else {
      partitionValues
    }
  }

  private def extractColName(v2Expr: V2Expression): Option[String] = v2Expr match {
    case f: V2FieldReference if f.fieldNames.length >= 1 => Some(f.fieldNames.quoted)
    case _ => None
  }
}
