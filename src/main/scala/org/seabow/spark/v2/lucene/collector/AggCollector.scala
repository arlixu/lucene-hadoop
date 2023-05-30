package org.seabow.spark.v2.lucene.collector

import org.apache.lucene.index.{LeafReader, LeafReaderContext, SortedNumericDocValues, SortedSetDocValues}
import org.apache.lucene.search.{ScoreMode, SimpleCollector}
import org.apache.lucene.util.NumericUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.execution.AggregatePushDownUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.LuceneFilters
import org.apache.spark.sql.v3.evolving.expressions.aggregate._
import org.apache.spark.sql.v3.evolving.util.V2ColumnUtils
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

class AggCollector(agg: Aggregation, aggSchema: StructType, dataSchema: StructType) extends SimpleCollector {
  var currentContext: LeafReaderContext = _
  var bucketMap = mutable.Map[InternalRow, InternalRow]().empty
  val groupBySchema = AggregatePushDownUtils.getSchemaWithoutAggregateExpression(aggSchema, agg)
  val aggMetricSchema = AggregatePushDownUtils.getSchemaWithoutGroupingExpression(aggSchema, agg)
  var docValueMaps: Map[String, Any] = Map.empty
  var nameDataTypeMap=LuceneFilters.nameToDataType(dataSchema)

  def getResultRows():mutable.Iterable[InternalRow]={
    val offset=groupBySchema.length
    val length=aggMetricSchema.length
      bucketMap.map{kv=>
        val resultRow=kv._1
        val metricRow=kv._2
        for(i<-0 until length)
        resultRow.update(offset+i,metricRow.get(i,aggMetricSchema(i).dataType))
        resultRow
    }
  }

  override def collect(doc: Int): Unit = {
    val result = new SpecificInternalRow(aggSchema)
    if (!agg.groupByExpressions().isEmpty) {
      //group by agg
      for (i <- 0 until (groupBySchema.length)) {
        groupBySchema(i).dataType match {
          case IntegerType|DateType =>
            val docValues = docValueMaps(groupBySchema(i).name).asInstanceOf[SortedNumericDocValues]
            if (docValues.advanceExact(doc)) {
              result.setInt(i, docValues.nextValue().toInt)
            }
          case LongType|TimestampType =>
            val docValues = docValueMaps(groupBySchema(i).name).asInstanceOf[SortedNumericDocValues]
            if (docValues.advanceExact(doc)) {
              result.setLong(i, docValues.nextValue())
            }
          case DoubleType =>
            val docValues = docValueMaps(groupBySchema(i).name).asInstanceOf[SortedNumericDocValues]
            if (docValues.advanceExact(doc)) {
              result.setDouble(i, NumericUtils.sortableLongToDouble(docValues.nextValue()))
            }
          case FloatType =>
            val docValues = docValueMaps(groupBySchema(i).name).asInstanceOf[SortedNumericDocValues]
            if (docValues.advanceExact(doc)) {
              result.setFloat(i, NumericUtils.sortableIntToFloat(docValues.nextValue().toInt))
            }
          case StringType =>
            val docValues = docValueMaps(groupBySchema(i).name).asInstanceOf[SortedSetDocValues]
            if (docValues.advanceExact(doc)) {
              result.update(i, UTF8String.fromString(docValues.lookupOrd(docValues.nextOrd()).utf8ToString()))
            }
        }
      }}
     val metricRow=  if(!bucketMap.contains(result)){
       val row=new SpecificInternalRow(aggMetricSchema)
        bucketMap.put(result,row)
       row
      }else{
       bucketMap.get(result).get
     }
      for (i <- 0 until agg.aggregateExpressions().length)
        agg.aggregateExpressions()(i) match {
          case _:Count | _:CountStar =>
            val oldData = if (metricRow.isNullAt(i)) {
              0l
            } else {
              metricRow.getLong(i)
            }
            metricRow.setLong(i, oldData + 1)
          case max: Max =>
            val col = V2ColumnUtils.extractV2Column(max.column()).get
            aggMetricSchema(i).dataType match {
              case LongType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setLong(i, docValues.nextValue())
                  } else {
                    metricRow.setLong(i, Math.max(metricRow.getLong(i), docValues.nextValue()))
                  }
                }
              case IntegerType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setInt(i, docValues.nextValue().toInt)
                  } else {
                    metricRow.setInt(i, Math.max(metricRow.getInt(i), docValues.nextValue().toInt))
                  }
                }
              case DoubleType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setDouble(i, NumericUtils.sortableLongToDouble(docValues.nextValue()))
                  } else {
                    metricRow.setDouble(i, Math.max(metricRow.getDouble(i), NumericUtils.sortableLongToDouble(docValues.nextValue())))
                  }
                }
              case FloatType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setFloat(i, NumericUtils.sortableIntToFloat(docValues.nextValue().toInt))
                  } else {
                    metricRow.setFloat(i, Math.max(metricRow.getFloat(i), NumericUtils.sortableIntToFloat(docValues.nextValue().toInt)))
                  }
                }
            }
          case min: Min =>
            val col = V2ColumnUtils.extractV2Column(min.column()).get
            aggMetricSchema(i).dataType match {
              case LongType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setLong(i, docValues.nextValue())
                  } else {
                    metricRow.setLong(i, Math.min(metricRow.getLong(i), docValues.nextValue()))
                  }
                }
              case IntegerType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setInt(i, docValues.nextValue().toInt)
                  } else {
                    metricRow.setInt(i, Math.min(metricRow.getInt(i), docValues.nextValue().toInt))
                  }
                }
              case DoubleType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setDouble(i, NumericUtils.sortableLongToDouble(docValues.nextValue()))
                  } else {
                    metricRow.setDouble(i, Math.min(metricRow.getDouble(i), NumericUtils.sortableLongToDouble(docValues.nextValue())))
                  }
                }
              case FloatType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setFloat(i, NumericUtils.sortableIntToFloat(docValues.nextValue().toInt))
                  } else {
                    metricRow.setFloat(i, Math.min(metricRow.getFloat(i), NumericUtils.sortableIntToFloat(docValues.nextValue().toInt)))
                  }
                }

            }
          case sum: Sum =>
            val col = V2ColumnUtils.extractV2Column(sum.column()).get
            aggMetricSchema(i).dataType match {
              case LongType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setLong(i, docValues.nextValue())
                  } else {
                    metricRow.setLong(i, metricRow.getLong(i)+docValues.nextValue())
                  }
                }
              case IntegerType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setInt(i, docValues.nextValue().toInt)
                  } else {
                    metricRow.setInt(i, metricRow.getInt(i)+ docValues.nextValue().toInt)
                  }
                }
              case DoubleType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setDouble(i, NumericUtils.sortableLongToDouble(docValues.nextValue()))
                  } else {
                    metricRow.setDouble(i, metricRow.getDouble(i)+ NumericUtils.sortableLongToDouble(docValues.nextValue()))
                  }
                }
              case FloatType =>
                val docValues = docValueMaps(col).asInstanceOf[SortedNumericDocValues]
                if (docValues.advanceExact(doc)) {
                  if (metricRow.isNullAt(i)) {
                    metricRow.setFloat(i, NumericUtils.sortableIntToFloat(docValues.nextValue().toInt))
                  } else {
                    metricRow.setFloat(i, metricRow.getFloat(i)+ NumericUtils.sortableIntToFloat(docValues.nextValue().toInt))
                  }
                }

            }
        }
  }

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    currentContext = context
    docValueMaps = getDocValuesMap(agg, dataSchema, context.reader())
  }

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES

  def getDocValuesMap(agg: Aggregation, dataSchema: StructType, reader: LeafReader): Map[String, Any] = {
    val groupCols = agg.groupByExpressions().map(V2ColumnUtils.extractV2Column).toSet
    val aggCols = agg.aggregateExpressions().map {
      case max: Max =>
        V2ColumnUtils.extractV2Column(max.column())
      case min: Min =>
        V2ColumnUtils.extractV2Column(min.column())
      case sum: Sum =>
        V2ColumnUtils.extractV2Column(sum.column())
      case _ => None
    }.toSet
    val colsSet = (groupCols ++ aggCols).filter(_.isDefined).map(col => StructField(col.get,LuceneFilters.attrDataType(col.get,nameDataTypeMap)))
    colsSet.map {
      col =>
        col.dataType match {
          case StringType =>
            (col.name, reader.getSortedSetDocValues(col.name))
          case DoubleType | LongType | IntegerType | FloatType =>
            (col.name, reader.getSortedNumericDocValues(col.name))
          case _ =>
            (col.name, reader.getSortedSetDocValues(col.name))
        }
    }.toMap
  }
}
