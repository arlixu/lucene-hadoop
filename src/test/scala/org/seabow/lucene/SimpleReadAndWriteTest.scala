package org.seabow.lucene

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.lucene._
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._

class SimpleReadAndWriteTest extends AnyFunSuite with Logging with SparkSessionTestWrapper with DatasetComparer with BeforeAndAfterAll{
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
      StructField("MapArray", MapType(StringType, ArrayType(StringType)), true),
      StructField("StructInfo", StructType(Seq(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("map_tags",ArrayType(StringType),true)
      )), true),
      StructField("ImpDay", IntegerType, true),
    )
  )
  val expectedData_01 = List(
    Row(false,Date.valueOf("2023-05-29"),Timestamp.valueOf("2023-05-29 00:00:01"),
      "CA869", "Phạm Uyển Trinh", null, null, 2200.01f, null, Array("1", "2", "3"),
      Map(),Map("sport"->Array("football", "basketball")), Row("John Doe", 30,Array("football", "basketball")),20230529),
    Row(true,Date.valueOf("2023-05-29"), Timestamp.valueOf("2023-05-29 00:00:02"),
      "CA870", "Nguyễn Liên Thảo", null, null, 2000.02f, 1350.05d, Array("1", "2", "3"),
      null,Map("sport"->Array("football")), Row("Jane Smith", 25,Array("football")),20230529),
    Row(false,Date.valueOf("2023-05-30"),Timestamp.valueOf("2023-05-30 00:00:01") ,
      "CA871", "Lê Thị Nga", 17000, null, null, null, Array("1", "2", "3"),
      Map("color" -> "yellow", "0.3" -> "1"),Map("sport"->Array("football", "basketball", "tennis")), Row("David Johnson", 40,Array("football", "basketball", "tennis")),20230530),
    Row(false,Date.valueOf("2023-05-31"),Timestamp.valueOf("2023-05-31 00:00:01") ,
      "CA872", "Phan Tố Nga", null, null, 2000.02f, null, Array("1", "2", "3"),
      Map("color" -> "blue", "0.3" -> "1"), Map(),Row("Sarah Williams", 35,Array()),20230531),
    Row(false,Date.valueOf("2023-06-01"),Timestamp.valueOf("2023-06-01 00:00:01") ,
      "CA873", "Nguyễn Thị Teresa Teng", null, 132324l, 1200.03f, 1350.06d,
      Array("1", "2", "3","4"), Map("color" -> "red", "0.5" -> "2"),null,
      Row("Michael Brown", 45,null),20230601)
  ).asJava

  val testDF=spark.createDataFrame(expectedData_01,userDefinedSchema_01)
  val hdfs=FileSystem.get(new Configuration)

  test("writeAndRead"){
     val expectedDF = testDF.orderBy("CustomerID")
      val actualDF=spark.read.lucene("spark_lucene").orderBy("CustomerID")
     assertSmallDatasetEquality(actualDF,expectedDF)
  }

  test("complexPushFilter"){
    val conditionExpr="`MapInfo`.color='red' and (Day>'2023-05-29' and array_contains(`ArrayInfo`,'3') )"
   val actualDF= spark.read.lucene("spark_lucene").filter(conditionExpr).orderBy("CustomerID")
   val expectedDF= testDF.filter(conditionExpr).orderBy("CustomerID")
    assertSmallDatasetEquality(actualDF,expectedDF)
    val expainString= actualDF.queryExecution.explainString(ExtendedMode)
    val shouldContainsStr = "IsNotNull(Day), IsNotNull(MapInfo), EqualTo(MapInfo.color,red), GreaterThan(Day,2023-05-29), EqualTo(ArrayInfo,3)"

    val pushedFiltersRegex = "PushedFilters:\\s*\\[(.*?)\\]".r
    val pushedFiltersMatch = pushedFiltersRegex.findFirstMatchIn(expainString)
    val pushedFilters = pushedFiltersMatch.map(_.group(1)).getOrElse("")
    val containsAllSubstrings = shouldContainsStr.split(", ").forall(substring => pushedFilters.contains(substring.trim))
    assert(containsAllSubstrings)

  }


  test("complexPushAgg"){

    val actualDF= spark.read.lucene("spark_lucene").groupBy("MapInfo.`color`").agg(
      count("ExtraOption3").as("cnt")
      , sum("ExtraOption3")).orderBy("color")
    val expectedDF=testDF.groupBy("MapInfo.`color`").agg(
      count("ExtraOption3").as("cnt")
      , sum("ExtraOption3")).orderBy("color")
    assertSmallDatasetEquality(actualDF,expectedDF)
    val expainString= actualDF.queryExecution.explainString(ExtendedMode)
    val shouldContainsStr="PushedAggregation:[Count(ExtraOption3), Sum(ExtraOption3)],PushedGroupBy:[MapInfo.color]"
    assert(expainString.contains(shouldContainsStr))
  }

  test("partition purge"){
    val actualDF= spark.read.lucene("spark_lucene").filter("ImpDay=20230529").orderBy("CustomerID")
    val expectedDF=testDF.filter("ImpDay=20230529").orderBy("CustomerID")
    assertSmallDatasetEquality(actualDF, expectedDF)
  }

  test("spark-sql create and read"){
    val ddl=testDF.schema.toDDL
    val createDDL= s"create table test_table ($ddl) using lucene"
    spark.sql(createDDL)
    testDF.write.insertInto("test_table")
    val conditionExpr="`MapInfo`.color='red' and (Day>'2023-05-29' and array_contains(`ArrayInfo`,'3') )"
    val selectSql= s"select * from `lucene`.`spark-warehouse/test_table/` where $conditionExpr"
    val actualDF=spark.sql(selectSql).orderBy("CustomerID")
    val expectedDF=testDF.filter(conditionExpr).orderBy("CustomerID")
    assertSmallDatasetEquality(actualDF, expectedDF)
  }

  test("facet group by array"){
    var actualDF=spark.read.option("enforceFacetSchema","true").lucene("spark_lucene").groupBy("ArrayInfo").count().orderBy("ArrayInfo")
    val expectedDF = testDF.withColumn("ArrayInfo", explode_outer(col("ArrayInfo"))).groupBy("ArrayInfo").count().orderBy("ArrayInfo")
    assertSmallDatasetEquality(actualDF,expectedDF)

  }

  test("facet group by map<x,array>"){
    val actualDF=spark.read.option("enforceFacetSchema","true").lucene("spark_lucene").groupBy("MapArray.sport").count().orderBy("sport")
    val expectedDF = testDF.withColumn("sport", explode_outer(col("MapArray.sport"))).groupBy("sport").count().orderBy("sport")
    assertSmallDatasetEquality(actualDF,expectedDF)
  }

  test("facet group by struct<array>"){
    val actualDF=spark.read.option("enforceFacetSchema","true").lucene("spark_lucene").groupBy("StructInfo.map_tags").count().orderBy("map_tags")
    val expectedDF = testDF.withColumn("map_tags", explode_outer(col("StructInfo.map_tags"))).groupBy("map_tags").count().orderBy("map_tags")
    assertSmallDatasetEquality(actualDF,expectedDF)
  }

  def clearData = {
    hdfs.delete(new Path("spark-warehouse"), true)
    hdfs.delete(new Path("spark_lucene"), true)
  }


  override def beforeAll(){
     println("beforeAll")
    clearData
    testDF.write.mode("overwrite").partitionBy("ImpDay").lucene("spark_lucene")
  }

  override def afterAll(): Unit ={
    println("afterAll")
    clearData

  }

}
