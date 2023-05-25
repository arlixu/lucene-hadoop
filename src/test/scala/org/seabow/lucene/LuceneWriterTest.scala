package org.seabow.lucene

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class LuceneWriterTest extends AnyFunSuite with Logging with SparkSessionTestWrapper {

  test("writeArray"){
    val userDefinedSchema_01 = StructType(
      List(
        StructField("isDeleted",BooleanType,true),
        StructField("Day", IntegerType, true),
        StructField("Customer ID", StringType, true),
        StructField("Customer Name", StringType, true),
        StructField("Standard Package", IntegerType, true),
        StructField("Extra Option 1", LongType, true),
        StructField("Extra Option 2", FloatType, true),
        StructField("Extra Option 3", DoubleType, true),
        StructField("Array Info", ArrayType(StringType), true),
        StructField("Map Info", MapType(StringType, StringType), true),
        StructField("Struct Info", StructType(Seq(
          StructField("name", StringType, true),
          StructField("age", IntegerType, true)
        )), true)
      )
    )

    val expectedData_01 = List(
      Row(false,1, "CA869", "Phạm Uyển Trinh", null, null, 2200.01f, null, Array("1", "2", "3"), Map(), Row("John Doe", 30)),
      Row(true,2, "CA870", "Nguyễn Liên Thảo", null, null, 2000.02f, 1350.05d, Array("1", "2", "3"), null, Row("Jane Smith", 25)),
      Row(false,4, "CA871", "Lê Thị Nga", 17000, null, null, null, Array("1", "2", "3"), Map("color" -> "yellow", "0.3" -> "1"), Row("David Johnson", 40)),
      Row(false,1, "CA872", "Phan Tố Nga", null, null, 2000.02f, null, Array("1", "2", "3"), Map(), Row("Sarah Williams", 35)),
      Row(false,1, "CA873", "Nguyễn Thị Teresa Teng", null, 132324l, 1200.03f, null, Array("1", "2", "3","4"), Map("color" -> "red", "0.5" -> "2"), Row("Michael Brown", 45))
    ).asJava


      val df=spark.createDataFrame(expectedData_01,userDefinedSchema_01)
      df.write.format("lucene").mode("overwrite").save("spark_lucene")
//      df.write.format("orc").mode("overwrite").save("spark_orc")
//      val indexPath=FileSystem.get(new Configuration()).listStatus(new Path("spark_lucene")).map(_.getPath.toString
//      ).filter(_.endsWith(".lucene")).head
//      new LuceneReader().searchIndex(indexPath)
  }

  test("readArray"){
    spark.read.format("lucene").load("spark_lucene").show(false)
  }

  test("complexPushFilter"){
   val df= spark.read.format("lucene").load("spark_lucene")
//     .groupBy("`Map Info`.color").count()
//     .filter("`Map Info`.color='red'")
     .filter(" (Day=1 and array_contains(`Array Info`,'4') )")
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

}
