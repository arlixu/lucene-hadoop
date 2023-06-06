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
  val groupRowSetters = groupBySchema.map(makeRowSetter)
  val aggRowSetters=(for (i<- 0 until aggMetricSchema.length) yield i).map{
    i=>
      makeAggRowSetter(agg.aggregateExpressions()(i),aggMetricSchema(i))
  }

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

  def getSingleDocValues(colName:String,dataType:DataType, doc: Int):Option[Any]={
    dataType match {
      case StringType =>
        val docValues = docValueMaps(colName).asInstanceOf[SortedSetDocValues]
        if (docValues != null && docValues.advanceExact(doc)) {
          Some(UTF8String.fromString(docValues.lookupOrd(docValues.nextOrd()).utf8ToString()))
        } else {
          None
        }
      case _ =>
        val docValues = docValueMaps(colName).asInstanceOf[SortedNumericDocValues]
        if (docValues != null && docValues.advanceExact(doc)) {
          {
            Some(docValues.nextValue())
          }
        } else {
          None
        }
    }
  }

  def getMultiDocValues(colName:String,dataType:DataType, doc: Int): Seq[Any] = {
    dataType match {
      case StringType =>
        val docValues = docValueMaps(colName).asInstanceOf[SortedSetDocValues]
        if (docValues != null && docValues.advanceExact(doc)) {
          for (j <- 0l until docValues.getValueCount()) yield {
            UTF8String.fromString(docValues.lookupOrd(docValues.nextOrd()).utf8ToString())
          }
        } else {
          Seq.empty
        }
      case _ =>
        val docValues = docValueMaps(colName).asInstanceOf[SortedNumericDocValues]
        if (docValues != null && docValues.advanceExact(doc)) {
          {
            for (j <- 0 until docValues.docValueCount()) yield {
              docValues.nextValue()
            }
          }
        } else {
          Seq.empty
        }
    }
  }

  protected type RowSetter = (InternalRow, Int, Any) => Unit
  protected def makeRowSetter(structField: StructField): RowSetter = structField.dataType match {
    case BooleanType =>
      (row: InternalRow, ordinal: Int, value: Any) =>
        row.update(ordinal, value.asInstanceOf[Long] > 0)
    case IntegerType | DateType =>
      (row: InternalRow, ordinal: Int, value: Any) =>
        row.setInt(ordinal, value.asInstanceOf[Long].toInt)
    case LongType | TimestampType =>
      (row: InternalRow, ordinal: Int, value: Any) =>
        row.setLong(ordinal, value.asInstanceOf[Long])
    case DoubleType =>
      (row: InternalRow, ordinal: Int, value: Any) =>
        row.setDouble(ordinal, NumericUtils.sortableLongToDouble(value.asInstanceOf[Long]))
    case FloatType =>
      (row: InternalRow, ordinal: Int, value: Any) =>
        row.setFloat(ordinal, NumericUtils.sortableIntToFloat(value.asInstanceOf[Long].toInt))
    case StringType|_ =>
      (row: InternalRow, ordinal: Int, value: Any) =>
        row.update(ordinal, value)
  }

  protected def makeAggRowSetter(aggregateFunc: AggregateFunc,structField:StructField):RowSetter={
    structField.dataType match {
      case LongType =>
        (row: InternalRow, ordinal: Int, value: Any) =>
          val oldValue = if(row.isNullAt(ordinal)) {None} else{Some(row.getLong(ordinal))}
          val currentValue =value.asInstanceOf[Long]
          val valueToUpdate=aggregateFunc match {
            case _:Max=> if(oldValue.isDefined)(Math.max(oldValue.get, currentValue))else(currentValue)
            case _:Min=> if(oldValue.isDefined)(Math.min(oldValue.get, currentValue))else(currentValue)
            case _:Sum=> if(oldValue.isDefined)(Math.addExact(oldValue.get, currentValue))else(currentValue)
            case _:Count|_:CountStar=> if(oldValue.isDefined)(Math.addExact(oldValue.get, 1))else(1)
          }
          row.setLong(ordinal, valueToUpdate)
      case IntegerType =>
        (row: InternalRow, ordinal: Int, value: Any) =>
          val oldValue = if(row.isNullAt(ordinal)) {None} else{Some(row.getInt(ordinal))}
          val currentValue =value.asInstanceOf[Long].toInt
          val valueToUpdate=aggregateFunc match {
            case _:Max=> if(oldValue.isDefined)(Math.max(oldValue.get, currentValue))else(currentValue)
            case _:Min=> if(oldValue.isDefined)(Math.min(oldValue.get, currentValue))else(currentValue)
            case _:Sum=> if(oldValue.isDefined)(Math.addExact(oldValue.get, currentValue))else(currentValue)
          }
          row.setLong(ordinal, valueToUpdate)
      case FloatType =>
        (row: InternalRow, ordinal: Int, value: Any) =>
          val oldValue = if(row.isNullAt(ordinal)) {None} else{Some(row.getFloat(ordinal))}
          val currentValue =NumericUtils.sortableIntToFloat(value.asInstanceOf[Long].toInt)
          val valueToUpdate=aggregateFunc match {
            case _:Max=> if(oldValue.isDefined)(Math.max(oldValue.get, currentValue))else(currentValue)
            case _:Min=> if(oldValue.isDefined)(Math.min(oldValue.get, currentValue))else(currentValue)
            case _:Sum=> if(oldValue.isDefined)(oldValue.get+currentValue)else(currentValue)
          }
          row.setFloat(ordinal, valueToUpdate)
      case DoubleType =>
        (row: InternalRow, ordinal: Int, value: Any) =>
          val oldValue = if(row.isNullAt(ordinal)) {None} else{Some(row.getDouble(ordinal))}
          val currentValue =NumericUtils.sortableLongToDouble(value.asInstanceOf[Long])
          val valueToUpdate=aggregateFunc match {
            case _:Max=> if(oldValue.isDefined)(Math.max(oldValue.get, currentValue))else(currentValue)
            case _:Min=> if(oldValue.isDefined)(Math.min(oldValue.get, currentValue))else(currentValue)
            case _:Sum=> if(oldValue.isDefined)(oldValue.get+currentValue)else(currentValue)
          }
          row.setDouble(ordinal, valueToUpdate)
    }
  }

  protected def updateMetricRow(doc: Int, metricRow: InternalRow) = {
    def aggWithDocValues(i: Int, col: String) = {
      val docValues = getSingleDocValues(col, aggMetricSchema(i).dataType, doc)
      if (docValues.isDefined) {
        aggRowSetters(i)(metricRow, i, docValues.get)
      }
    }

    for (i <- 0 until agg.aggregateExpressions().length) {
      agg.aggregateExpressions()(i) match {
        case _: CountStar =>
          aggRowSetters(i)(metricRow, i, 1l)
        case count: Count =>
          val col = V2ColumnUtils.extractV2Column(count.column()).get
          val docValues = getSingleDocValues(col, aggMetricSchema(i).dataType, doc)
          if (docValues.isDefined) {
            aggRowSetters(i)(metricRow, i, 1l)
          }
        case max: Max =>
          val col = V2ColumnUtils.extractV2Column(max.column()).get
          aggWithDocValues(i, col)
        case min: Min =>
          val col = V2ColumnUtils.extractV2Column(min.column()).get
          aggWithDocValues(i, col)
        case sum: Sum =>
          val col = V2ColumnUtils.extractV2Column(sum.column()).get
          aggWithDocValues(i, col)
      }
    }
  }

  override def collect(doc: Int): Unit = {
    val result = new SpecificInternalRow(aggSchema)
    if (!agg.groupByExpressions().isEmpty) {
      //group by agg
      for (i <- 0 until (groupBySchema.length)) {
        val docValues=getSingleDocValues(groupBySchema(i).name,groupBySchema(i).dataType,doc)
        if(docValues.isDefined){
          groupRowSetters(i)(result,i,docValues.get)
        }
      }}
     val metricRow=  if(!bucketMap.contains(result)){
       val row=new SpecificInternalRow(aggMetricSchema)
        bucketMap.put(result,row)
       row
      }else{
       bucketMap.get(result).get
     }
    updateMetricRow(doc,metricRow)
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
