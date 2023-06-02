package org.apache.spark.sql.v2.lucene

import org.apache.lucene.document.{DoublePoint, FloatPoint, IntPoint, LongPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.util.BytesRef
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

object LuceneFilters {

  def convertToWildcard(str: String): String = {
    val backtickPattern = "\\.`([^`]*)`"
    val doubleDotPattern = "\\.[^\\.]+"
    val replacedStr = str.replaceAll(backtickPattern, "\\.*")
    val finalStr = replacedStr.replaceAll(doubleDotPattern, "\\.*")
    finalStr
  }
  private def dateToDays(date: Any): SQLDate = date match {
    case d: Date => DateTimeUtils.fromJavaDate(d)
    case ld: LocalDate => DateTimeUtils.localDateToDays(ld)
  }

  private def timestampToMicros(v: Any): Long = v match {
    case i: Instant => DateTimeUtils.instantToMicros(i)
    case t: Timestamp => DateTimeUtils.fromJavaTimestamp(t)
  }

   case class AtomicField(fieldNames: Array[String],fieldType: DataType)

  //  private def getDataTypeBy
   def nameToDataType(schema: StructType): Map[String, DataType] = {
    def getAtomicFields(fields: Seq[StructField],
                        parentFieldNames: Array[String] = Array.empty): Seq[AtomicField] = {
      fields.flatMap{
        field=>
          field.dataType match {
            case t:ArrayType =>
              getAtomicFields(Seq(StructField(field.name,t.elementType)),parentFieldNames)
            case t:MapType=>
               t.valueType match {
                 case v:AtomicType=>
                   Seq(AtomicField(parentFieldNames:+field.name:+"*",v))
                 case _=>
                   getAtomicFields(Seq(StructField("*",t.valueType)),parentFieldNames:+field.name)
               }
            case t:StructType=>
              getAtomicFields(t.fields,parentFieldNames:+field.name)
            case t:AtomicType=>
              Seq(AtomicField(parentFieldNames:+field.name,t))
            case _ => None
          }

      }
    }
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    val atomicFields = getAtomicFields(schema.fields).map { field =>
      (field.fieldNames.toSeq.quoted, field.fieldType)
    }
    atomicFields.toMap
  }

  def createFilter(schema: StructType, filters: Seq[Filter]): Query = {
    if (filters.isEmpty) {
      return new MatchAllDocsQuery()
    }
    val builder = new BooleanQuery.Builder()
    builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
    val nameTypeMap=nameToDataType(schema)
    filters.foreach {
      case EqualTo(attr, value) =>
        val query = equalToQuery(nameTypeMap, attr, value)
        builder.add(query, BooleanClause.Occur.MUST)
      case GreaterThan(attr, value) =>
        val fieldName = attr
        val query = attrDataType(attr,nameTypeMap) match {
          case IntegerType =>
            IntPoint.newRangeQuery(fieldName, Math.addExact(value.asInstanceOf[Int], 1), Int.MaxValue)
          case LongType =>
            LongPoint.newRangeQuery(fieldName, Math.addExact(value.asInstanceOf[Long], 1), Long.MaxValue)
          case DateType=>
            IntPoint.newRangeQuery(fieldName, Math.addExact(dateToDays(value), 1), Int.MaxValue)
          case TimestampType =>
            LongPoint.newRangeQuery(fieldName, Math.addExact(timestampToMicros(value), 1), Long.MaxValue)
          case FloatType =>
            FloatPoint.newRangeQuery(fieldName, Math.nextUp(value.asInstanceOf[Float]), Float.PositiveInfinity)
          case DoubleType =>
            DoublePoint.newRangeQuery(fieldName, Math.nextUp(value.asInstanceOf[Double]), Double.PositiveInfinity)
          case StringType | _ =>
            TermRangeQuery.newStringRange(fieldName, value.toString, null, false, false)
        }
        builder.add(query, BooleanClause.Occur.MUST)
      case GreaterThanOrEqual(attr, value) =>
        val fieldName = attr
        val query = attrDataType(attr,nameTypeMap) match {
          case IntegerType =>
            IntPoint.newRangeQuery(fieldName, value.asInstanceOf[Int], Int.MaxValue)
          case LongType =>
            LongPoint.newRangeQuery(fieldName, value.asInstanceOf[Long], Long.MaxValue)
          case DateType=>
            IntPoint.newRangeQuery(fieldName, dateToDays(value), Int.MaxValue)
          case TimestampType =>
            LongPoint.newRangeQuery(fieldName, timestampToMicros(value), Long.MaxValue)
          case FloatType =>
            FloatPoint.newRangeQuery(fieldName, value.asInstanceOf[Float], Float.PositiveInfinity)
          case DoubleType =>
            DoublePoint.newRangeQuery(fieldName, value.asInstanceOf[Double], Double.PositiveInfinity)
          case StringType | _ =>
            TermRangeQuery.newStringRange(fieldName, value.toString, null, true, false)
        }
        builder.add(query, BooleanClause.Occur.MUST)

      case LessThan(attr, value) =>
        val fieldName = attr
        val query = attrDataType(attr,nameTypeMap) match {
          case IntegerType =>
            IntPoint.newRangeQuery(fieldName, Int.MinValue, Math.addExact(value.asInstanceOf[Int], -1))
          case LongType =>
            LongPoint.newRangeQuery(fieldName, Long.MinValue, Math.addExact(value.asInstanceOf[Long], -1))
          case DateType=>
            IntPoint.newRangeQuery(fieldName, Int.MinValue, Math.addExact(dateToDays(value), -1))
          case TimestampType =>
            LongPoint.newRangeQuery(fieldName, Long.MinValue, Math.addExact(timestampToMicros(value), -1))
          case FloatType =>
            FloatPoint.newRangeQuery(fieldName, Float.NegativeInfinity, Math.nextDown(value.asInstanceOf[Float]))
          case DoubleType =>
            DoublePoint.newRangeQuery(fieldName, Double.NegativeInfinity, Math.nextDown(value.asInstanceOf[Double]))
          case StringType | _ =>
            TermRangeQuery.newStringRange(fieldName, null, value.toString, false, false)
        }
        builder.add(query, BooleanClause.Occur.MUST)
      case LessThanOrEqual(attr, value) =>
        val fieldName = schema(attr).name
        val query = attrDataType(attr,nameTypeMap) match {
          case IntegerType =>
            IntPoint.newRangeQuery(fieldName, Int.MinValue, value.asInstanceOf[Int])
          case LongType =>
            LongPoint.newRangeQuery(fieldName, Long.MinValue, value.asInstanceOf[Long])
          case DateType=>
            IntPoint.newRangeQuery(fieldName, Int.MinValue, dateToDays(value))
          case TimestampType =>
            LongPoint.newRangeQuery(fieldName, Long.MinValue, timestampToMicros(value))
          case FloatType =>
            FloatPoint.newRangeQuery(fieldName, Float.NegativeInfinity, value.asInstanceOf[Float])
          case DoubleType =>
            DoublePoint.newRangeQuery(fieldName, Double.NegativeInfinity, value.asInstanceOf[Double])
          case StringType | _ =>
            TermRangeQuery.newStringRange(fieldName, null, value.toString, false, true)
        }
        builder.add(query, BooleanClause.Occur.MUST)
      case In(attr, values) =>
        val orQuery = new BooleanQuery.Builder()
        values.map(equalToQuery(nameTypeMap, attr, _)).foreach(orQuery.add(_, BooleanClause.Occur.SHOULD))
        builder.add(orQuery.build(), BooleanClause.Occur.MUST)
      case StringStartsWith(attr, value) =>
        val fieldName = attr
        builder.add(new PrefixQuery(new Term(fieldName, new BytesRef(value))), BooleanClause.Occur.MUST)
      case StringEndsWith(attr, value) =>
        val fieldName = attr
        builder.add(new WildcardQuery(new Term(fieldName, s"*${value}")), BooleanClause.Occur.MUST)
      case StringContains(attr, value) =>
        val fieldName = attr
        builder.add(new WildcardQuery(new Term(fieldName, s"*${value}*")), BooleanClause.Occur.MUST)
      case IsNull(attr) =>
        val fieldName = attr
        builder.add(new TermQuery(new Term("_field_names", new BytesRef(fieldName))), BooleanClause.Occur.MUST_NOT)
      case IsNotNull(attr) =>
        val fieldName = attr
        builder.add(new TermQuery(new Term("_field_names", new BytesRef(fieldName))), BooleanClause.Occur.MUST)
      case And(left, right) =>
        builder.add(new BooleanClause(createFilter(schema, Seq(left)), BooleanClause.Occur.MUST))
        builder.add(new BooleanClause(createFilter(schema, Seq(right)), BooleanClause.Occur.MUST))
      case Or(left, right) =>
        builder.add(new BooleanClause(createFilter(schema, Seq(left)), BooleanClause.Occur.SHOULD))
        builder.add(new BooleanClause(createFilter(schema, Seq(right)), BooleanClause.Occur.SHOULD))
      case Not(child) =>
        builder.add(new BooleanClause(createFilter(schema, Seq(child)), BooleanClause.Occur.MUST_NOT))
      case _ => //unsupported filter
    }
    val query = builder.build()
    query
  }

  private def equalToQuery(nameTypeMap: Map[String,DataType], attr: String, value: Any): Query = {
    val query = attrDataType(attr,nameTypeMap) match {
      case BooleanType=>
        val intValue= if(value.asInstanceOf[Boolean]) 1 else 0
        IntPoint.newExactQuery(attr,intValue)
      case IntegerType =>
        IntPoint.newExactQuery(attr, value.asInstanceOf[Int])
      case DateType=>
        IntPoint.newExactQuery(attr, dateToDays(value))
      case TimestampType =>
        LongPoint.newExactQuery(attr,timestampToMicros(value))
      case LongType =>
        LongPoint.newExactQuery(attr, value.asInstanceOf[Long])
      case FloatType =>
        FloatPoint.newExactQuery(attr, value.asInstanceOf[Float])
      case DoubleType =>
        DoublePoint.newExactQuery(attr, value.asInstanceOf[Double])
      case StringType | _ =>
        val term = new Term(attr, new BytesRef(value.toString))
        new TermQuery(term)
    }
    query
  }

  def attrDataType(attr:String,nameTypeMap: Map[String,DataType]):DataType={
      nameTypeMap.getOrElse(attr,nameTypeMap.get(convertToWildcard(attr)).get)
  }


}
