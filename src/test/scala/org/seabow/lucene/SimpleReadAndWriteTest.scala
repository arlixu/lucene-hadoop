package org.seabow.lucene

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.lucene._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._

class SimpleReadAndWriteTest extends AnyFunSuite with Logging with SparkSessionTestWrapper {
  val userDefinedSchema_01 = StructType(
    List(
      StructField("isDeleted",BooleanType,true),
      StructField("Day", DateType, true),
      StructField("Timestamp", TimestampType, true),
      StructField("CustomerID", StringType, true),
      StructField("CustomerName", StringType, true),
      StructField("StandardPackage", IntegerType, true),
      StructField("ExtraOption1", LongType, true),
      StructField("ExtraOption2", FloatType, true),
      StructField("ExtraOption3", DoubleType, true),
      StructField("ArrayInfo", ArrayType(StringType), true),
      StructField("MapInfo", MapType(StringType, StringType), true),
      StructField("StructInfo", StructType(Seq(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )), true)
    )
  )
  val expectedData_01 = List(
    Row(false,Date.valueOf("2023-05-29"),Timestamp.valueOf("2023-05-29 00:00:01"),"CA869", "Phạm Uyển Trinh", null, null, 2200.01f, null, Array("1", "2", "3"), Map(), Row("John Doe", 30)),
    Row(true,Date.valueOf("2023-05-29"), Timestamp.valueOf("2023-05-29 00:00:02"),"CA870", "Nguyễn Liên Thảo", null, null, 2000.02f, 1350.05d, Array("1", "2", "3"), null, Row("Jane Smith", 25)),
    Row(false,Date.valueOf("2023-05-30"),Timestamp.valueOf("2023-05-30 00:00:01") ,"CA871", "Lê Thị Nga", 17000, null, null, null, Array("1", "2", "3"), Map("color" -> "yellow", "0.3" -> "1"), Row("David Johnson", 40)),
    Row(false,Date.valueOf("2023-05-31"),Timestamp.valueOf("2023-05-31 00:00:01") ,"CA872", "Phan Tố Nga", null, null, 2000.02f, null, Array("1", "2", "3"), Map(), Row("Sarah Williams", 35)),
    Row(false,Date.valueOf("2023-06-01"),Timestamp.valueOf("2023-06-01 00:00:01") ,"CA873", "Nguyễn Thị Teresa Teng", null, 132324l, 1200.03f, null, Array("1", "2", "3","4"), Map("color" -> "red", "0.5" -> "2"), Row("Michael Brown", 45))
  ).asJava


  test("write"){
      val df=spark.createDataFrame(expectedData_01,userDefinedSchema_01)
      df.write.mode("overwrite").lucene("spark_lucene/partition=1")
  }

  test("read"){
    spark.read.format("lucene").load("spark_lucene").select("Timestamp").show(false)
  }

  test("complexPushFilter"){
   val df= spark.read.format("lucene").load("spark_lucene")
//     .groupBy("`Map Info`.color").count()
//     .filter("`Map Info`.color='red'")
     .filter(" (Day>'2023-05-29' and array_contains(`Array Info`,'3') )")
//     .filter("`Customer ID`='CA869'")
//   val df= spark.read.format("orc").load("spark_orc").filter("infos.name='Michael Brown'")
    df.printSchema()

    df.explain(true)
    df.show(false)
  }

  test("complexPushFilter map"){
    val df= spark.read.format("lucene").load("spark_lucene")
      //     .groupBy("`Map Info`.color").count()
      //     .filter("`Map Info`.color='red'")
      .filter(" `Map Info`.`0.5`='2'")
    //     .filter("`Customer ID`='CA869'")
    //   val df= spark.read.format("orc").load("spark_orc").filter("infos.name='Michael Brown'")
    df.printSchema()

    df.explain(true)
    df.show(false)
  }

  test("complexPushFilter count"){

    val df= spark.read.format("lucene").load("spark_lucene").groupBy("Array Info").agg(
      count("Extra Option 3")
      , sum("Extra Option 3")
//      ,avg("Extra Option 3")
    )
      //     .filter("`Map Info`.color='red'")
//      .groupBy().sum("Day")
    //     .filter("`Customer ID`='CA869'")
    //   val df= spark.read.format("orc").load("spark_orc").filter("infos.name='Michael Brown'")
    df.printSchema()

//    df.explain(true)
    df.show(false)
  }

  test("partition purge"){
    val df= spark.read.format("lucene").load("spark_lucene").filter("partition=2")
    df.explain(true)
    df.show(false)
  }

  test("spark-sql"){
    val df=spark.createDataFrame(expectedData_01,userDefinedSchema_01)
    val ddl=df.schema.toDDL
    val createDDL= s"create table test_table ($ddl) using lucene"
    spark.sql(createDDL)
    df.write.insertInto("test_table")
  }
  test("spark-sql select "){
    val selectSql= s"select count(*) from `lucene`.`spark-warehouse/test_table/` where `Map Info`.`0.5`='2' and Day>'2023-05-29'"
    spark.sql(selectSql).explain(true)
  }
}
