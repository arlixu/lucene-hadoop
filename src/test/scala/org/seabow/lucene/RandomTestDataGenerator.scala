package org.seabow.lucene

import org.apache.spark.sql.functions._
import org.apache.spark.sql.lucene._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object RandomTestDataGenerator extends AnyFunSuite with SparkSessionTestWrapper{
  val numRecords = 100000000
  val outputFilePath = "random_data_orc"
  test("generate") {
    // 创建Spark会话
    // 定义数据量和保存路径
    // 定义标签体系
    val tagHierarchy = Map(
      "sports" -> Array("football", "basketball", "tennis", "golf", "baseball", "swimming", "hockey", "cricket", "rugby", "volleyball"),
      "music" -> Array("rock", "pop", "jazz", "country", "hip-hop", "classical", "blues", "reggae", "metal", "folk"),
      "travel" -> Array("beach", "mountain", "city", "desert", "island", "countryside", "lake", "forest", "historical", "adventure"),
      "food" -> Array("pizza", "sushi", "burger", "pasta", "steak", "seafood", "salad", "tacos", "curry", "dim sum"),
      "technology" -> Array("smartphone", "laptop", "gadget", "tablet", "smartwatch", "headphones", "camera", "drone", "VR", "robot"),
      "fashion" -> Array("clothing", "shoes", "accessories", "handbags", "jewelry", "watches", "eyewear", "perfume", "cosmetics", "hats"),
      "books" -> Array("fiction", "non-fiction", "mystery", "romance", "thriller", "biography", "science fiction", "history", "fantasy", "self-help"),
      "movies" -> Array("action", "comedy", "drama", "horror", "romantic", "sci-fi", "adventure", "animation", "documentary", "thriller"),
      "fitness" -> Array("yoga", "running", "cycling", "weightlifting", "swimming", "pilates", "boxing", "zumba", "aerobics", "kickboxing"),
      "art" -> Array("painting", "sculpture", "photography", "drawing", "ceramics", "installation", "performance", "collage", "printmaking", "video")
    )
    val tagBroadcastMap=spark.sparkContext.broadcast[Map[String,Array[String]]](tagHierarchy)
    import spark.implicits._
    // 根据一级标签和标签体系生成二级标签的Map
    def generateMapTags( tagHierarchy: Map[String, Array[String]]): Map[String, Array[String]] = {
      tagHierarchy.filter(kv=>Random.nextDouble()>0.5d).map(kv=>
        (kv._1->kv._2.filter(a=>Random.nextDouble()>0.5d))
      ).filter(kv=>kv._2.nonEmpty)
    }


    def generateMapTagsUDF=udf(()=>
      generateMapTags(tagBroadcastMap.value))
    // 生成用户ID
    val df = spark.range(0, numRecords).select(col("id").cast("string").as("user_id"))

    // 生成用户属性
    val dfWithAttributes = df.withColumn("age", (rand() * 70).cast("int")
    ).withColumn("gender", (rand() < 0.5).cast("boolean")
    ).withColumn("region", (rand() * 100).cast("int")
    ).withColumn("occupation", (rand() * 5).cast("int"))

    // 转换为Map[String, Array[String]]格式
    val dfWithMapTags = dfWithAttributes.withColumn("map_tags", generateMapTagsUDF())
    dfWithMapTags.show(false)
    // 保存为ORC文件
    dfWithMapTags.write.mode("overwrite").orc(outputFilePath)
  }


  test("write orc/lucene compared"){
    var startTime = System.currentTimeMillis
    spark.read.orc(outputFilePath).write.orc("compared_orc")
    var endTime = System.currentTimeMillis
    var cost=(endTime-startTime)/1000
    println("cost secs orc write:"+cost)
    startTime = System.currentTimeMillis
    spark.read.orc(outputFilePath).write.mode("overwrite").lucene("compared_lucene")
    endTime = System.currentTimeMillis
    cost=(endTime-startTime)/1000
    println("cost secs lucene write:"+cost)

  }

  test("read from orc/lucene"){
    val condition="array_contains(map_tags.`sports`,'basketball')"
//    val condition="map_tags['sports'] is not null"
    var startTime = System.currentTimeMillis
    spark.read.orc("compared_orc").filter(condition).count()
    var endTime = System.currentTimeMillis
    var cost=(endTime-startTime)/1000
    println("cost secs orc read:"+cost)
    startTime = System.currentTimeMillis
    spark.read.lucene("compared_lucene").filter(condition).count()
    endTime = System.currentTimeMillis
    cost=(endTime-startTime)/1000
    println("cost secs lucene read:"+cost)
  }

  test("facet from orc/lucene"){
    val condition="array_contains(map_tags.`sports`,'basketball')"
//        val condition="map_tags['sports'] is not null"
    var startTime = System.currentTimeMillis
    spark.read.orc("compared_orc").filter(condition).selectExpr("map_tags.`art` as art").withColumn("art",explode_outer(col("art"))).groupBy("art").count().show
    var endTime = System.currentTimeMillis
    var cost=(endTime-startTime)/1000
    println("cost secs orc read:"+cost)
    startTime = System.currentTimeMillis
    spark.read.option("enforceFacetSchema","true").lucene("compared_lucene").filter("map_tags.`sports`='basketball'").groupBy("map_tags.`art`").count().show
    endTime = System.currentTimeMillis
    cost=(endTime-startTime)/1000
    println("cost secs lucene read:"+cost)
  }

}
