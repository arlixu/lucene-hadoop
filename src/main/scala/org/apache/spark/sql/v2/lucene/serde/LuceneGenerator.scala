package org.apache.spark.sql.v2.lucene.serde

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.lucene87.Lucene87Codec
import org.apache.lucene.codecs.lucene87.Lucene87Codec.Mode
import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.util.{BytesRef, NumericUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.seabow.HdfsDirectory
import org.seabow.spark.v2.lucene.LuceneOptions

import java.io.CharArrayWriter

class LuceneGenerator(val path: String, val dataSchema: StructType, val conf: Configuration, val options: LuceneOptions) {
  val (dir, writer) = {
    val dir = new HdfsDirectory(new Path(path), new Configuration)
    // 分析器
    val analyzer = new StandardAnalyzer
    // 索引配置
    val iwc = new IndexWriterConfig(analyzer)
    iwc.setOpenMode(OpenMode.CREATE_OR_APPEND)
    iwc.setCodec(new Lucene87Codec(Mode.BEST_SPEED))
    iwc.setUseCompoundFile(true)
    iwc.setRAMBufferSizeMB(128)
    iwc.setMaxBufferedDocs(1024*1024)
    // 索引写入器
    val writer = new IndexWriter(dir, iwc)
    (dir, writer)
  }


  def writeSchema():Unit={
    val hdfs=dir.getFileSystem
    val schema=new Path(path+"/.schema")
    if(!hdfs.exists(schema)){
      val dos=hdfs.create(schema)
      val bytes=dataSchema.toDDL.getBytes()
      dos.write(bytes, 0, bytes.length)
      dos.close()
    }
  }


  val charArrayWriter = new CharArrayWriter()
   val jsonGens:Seq[Option[JacksonGenerator]]= dataSchema.map{
      structField=> structField.dataType match {
        case ArrayType(elementType, _)=>
          Some(new JacksonGenerator(
            structField.dataType, charArrayWriter, new JSONOptions(Map.empty, options.zoneId.toString)))
        case MapType(keyType, valueType, _)=>
          Some(new JacksonGenerator(
            structField.dataType, charArrayWriter, new JSONOptions(Map.empty, options.zoneId.toString)))
        case StructType(fields)=>
          Some(new JacksonGenerator(
            structField.dataType, charArrayWriter, new JSONOptions(Map.empty, options.zoneId.toString)))
        case _=>None
      }
    }

  def getAndReset(gen:JacksonGenerator): UTF8String = {
    gen.flush()
    val json = charArrayWriter.toString
    charArrayWriter.reset()
    UTF8String.fromString(json)
  }


  private type ValueConverter = (SpecializedGetters, Int,Document) => Unit
  private val valueConverters: Array[ValueConverter] = dataSchema.map(makeConverter(_)).toArray


  private def makeConverter(structField: StructField,isRoot:Boolean=true): ValueConverter = {
    structField.dataType match {
    case BooleanType=>
        (row: SpecializedGetters, ordinal: Int, doc: Document) => {
          val intValue= if(row.getBoolean(ordinal)) 1 else 0
          doc.add(new IntPoint(structField.name, row.getInt(ordinal)))
          doc.add(new SortedNumericDocValuesField(structField.name,intValue))
          if(isRoot){
            doc.add(new StoredField(structField.name, intValue))
          }
        }
    case IntegerType =>
      (row: SpecializedGetters, ordinal: Int, doc: Document) => {
        doc.add(new IntPoint(structField.name, row.getInt(ordinal)))
        doc.add(new SortedNumericDocValuesField(structField.name, row.getInt(ordinal).toLong))
        if(isRoot){
          doc.add(new StoredField(structField.name, row.getInt(ordinal)))
        }
      }
    case LongType =>
      (row: SpecializedGetters, ordinal: Int, doc: Document) => {
        doc.add(new LongPoint(structField.name, row.getLong(ordinal)))
        doc.add(new SortedNumericDocValuesField(structField.name, row.getLong(ordinal)))
        if(isRoot) {
          doc.add(new StoredField(structField.name, row.getLong(ordinal)))
        }
      }
    case FloatType =>
      (row: SpecializedGetters, ordinal: Int, doc: Document) => {
        doc.add(new FloatPoint(structField.name, row.getFloat(ordinal)))
        doc.add(new SortedNumericDocValuesField(structField.name, NumericUtils.floatToSortableInt( row.getFloat(ordinal))))
        if(isRoot) {
          doc.add(new StoredField(structField.name, row.getFloat(ordinal)))
        }
      }
    case DoubleType =>
      (row: SpecializedGetters, ordinal: Int, doc: Document) => {
        doc.add(new DoublePoint(structField.name, row.getDouble(ordinal)))
        doc.add(new SortedNumericDocValuesField(structField.name, NumericUtils.doubleToSortableLong(row.getDouble(ordinal))))
        if(isRoot) {
          doc.add(new StoredField(structField.name, row.getDouble(ordinal)))
        }
      }
    case StringType =>
      (row: SpecializedGetters, ordinal: Int, doc: Document) => {
        var isStore=Field.Store.YES
        if(!isRoot){
          isStore=Field.Store.NO
        }
        doc.add(new StringField(structField.name, row. getUTF8String(ordinal).toString, isStore))
        doc.add(new SortedSetDocValuesField(structField.name, new BytesRef(row.getUTF8String(ordinal).toString)))
      }
    case ArrayType(elementType, _) => (row: SpecializedGetters, ordinal: Int, doc: Document)  =>{
      // Need to put all converted values to a list, can't reuse object.
      val array = row.getArray(ordinal)
      var i = 0
      while (i < array.numElements()) {
        if (!array.isNullAt(i)) {
          val elementConverter = makeConverter(StructField(structField.name, elementType, nullable = true),false)
          elementConverter(array, i, doc)
          }
        i += 1
      }
      if(isRoot){
        val jsonGenerator=jsonGens(ordinal).get
        jsonGenerator.write(array)
        val json=getAndReset(jsonGenerator)
        doc.add(new StoredField(structField.name,json.toString))
      }
    }

    case MapType(keyType, valueType, _) => (row: SpecializedGetters, ordinal: Int, doc: Document) =>{
      val map = row.getMap(ordinal)
      val length =map.numElements()
      val keys =map.keyArray()
      val values = map.valueArray()
      var i = 0
      while (i < length) {
        val key=keys.get(i,keyType)
        if(!values.isNullAt(i)){
         val kvConverter= makeConverter(StructField(Array(structField.name,key.toString).quoted, valueType, nullable = true),false)
          kvConverter(values,i,doc)
        }
        i += 1
      }
      if(isRoot){
        val jsonGenerator=jsonGens(ordinal).get
        jsonGenerator.write(map)
        val json=getAndReset(jsonGenerator)
        doc.add(new StoredField(structField.name,json.toString))
      }
    }
    case st: StructType =>  (row: SpecializedGetters, ordinal: Int, doc: Document) =>{
      val struct= row.getStruct(ordinal,st.size)
      val numFields = st.length
      var i = 0
      while (i < numFields) {
        if (!struct.isNullAt(i)) {
          val structConverter= makeConverter(StructField(Array(structField.name,st(i).name).quoted, st(i).dataType, nullable = true),false)
          structConverter(struct,i,doc)
        }
        i += 1
      }
      if(isRoot){
        val jsonGenerator=jsonGens(ordinal).get
        jsonGenerator.write(struct)
        val json=getAndReset(jsonGenerator)
        doc.add(new StoredField(structField.name,json.toString))
      }
    }

    case _ => throw new RuntimeException(s"Unsupported type: ${structField.dataType.typeName}")
    }
  }

  def write(row: InternalRow): Unit = {
    val doc = new Document
    for (idx <- 0 until row.numFields) {
      if (!row.isNullAt(idx)) {
        valueConverters(idx)(row, idx, doc)
        doc.add(new StringField( "_field_names",dataSchema(idx).name, Field.Store.NO))
      }
    }
    writer.addDocument(doc)
  }

  def close(): Unit = {
    jsonGens.filter(!_.isEmpty).foreach{
      _.get.close()
    }
    charArrayWriter.close()
    writer.flush()
    writer.commit()
//    writer.close()
//    dir.close()
  }
}
