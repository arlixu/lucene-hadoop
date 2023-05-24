package org.seabow.lucene

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.testing.memory","2718592000")
//      .config("spark.executor.memoryOverhead","2048")
//      .config("spark.executor.extraJavaOptions","-XX:MaxDirectMemorySize=2g -XX:+UseLargePages")
      //      .config("spark.files.maxPartitionBytes","10000000")
      .config("spark.local.dir","target/tmp")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }
}