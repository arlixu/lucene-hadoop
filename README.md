# lucene-hadoop
A Lucene Datasource of Spark.Supports push down filter and push down aggeragation.Test on spark version 3.0.2

## Demo
```
 val userDefinedSchema_01 = StructType(
      List(
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
      Row(1, "CA869", "Phạm Uyển Trinh", null, null, 2200.01f, null, Array("1", "2", "3"), Map(), Row("John Doe", 30)),
      Row(2, "CA870", "Nguyễn Liên Thảo", null, null, 2000.02f, 1350.05d, Array("1", "2", "3"), null, Row("Jane Smith", 25)),
      Row(4, "CA871", "Lê Thị Nga", 17000, null, null, null, Array("1", "2", "3"), Map("color" -> "yellow", "0.3" -> "1"), Row("David Johnson", 40)),
      Row(1, "CA872", "Phan Tố Nga", null, null, 2000.02f, null, Array("1", "2", "3"), Map(), Row("Sarah Williams", 35)),
      Row(1, "CA873", "Nguyễn Thị Teresa Teng", null, 132324l, 1200.03f, null, Array("1", "2", "3","4"), Map("color" -> "red", "0.5" -> "2"), Row("Michael Brown", 45))
    ).asJava


      val df=spark.createDataFrame(expectedData_01,userDefinedSchema_01)
      df.write.format("lucene").mode("overwrite").save("spark_lucene")
      val df= spark.read.format("lucene").load("spark_lucene")
 ```

## Supported Features
1. Read and write data with schema using spark-sql/dataframe api,as beyond.  
2. Pushed filters support not only for `atomic` types (`int,string..`) but also for `complex` types. e.g. `Map<String,String>. Array. StructType`. 
If you have a tags:Map<String,String> field. lucene hadoop will allow you to search for all keys of the `tags` field.
just like
```
df.filter("tags.`your_key`='you_value'")
```
And you can also push filters for `array_contains()` expression as follows:
```
df.filter("array_contains(your_array_col,'you_value')")
```
Both of the above cases will be converted to a TermQuery of Lucene query.  

Of course you can also push filters for any other expression. e.g. `>`,`<`,`in`...  

3. Push down aggregation support for `count,max,min,sum` by lucene `DocValuesField`. (Due to the limitations of Spark 3.0.2, aggregation push down for `avg` is not supported.The alternative solution is to calculate the sum and count first, and then divide them.)  
4. Prefer location implements for partition file.  
When writing a dataframe as `lucene` format,each partition file will be written as a lucene index dir on hdfs,with a suffix `.lucene`.  
Since opening an index in Lucene is a relatively expensive operation, we want to perform the index opening only when loading the Lucene-format data source for the first time. This requires ensuring that queries for the corresponding partition files are allocated to a fixed executor as much as possible.  
5. Support facets accelerate for arrayType<atomicTypes>.
```
spark.read.option("enforceFacetSchema","true").lucene("lucene_path").groupBy("ArrayInfo").count()
```
same as 
```
spark.read.orc("orc_path"
).withColumn("ArrayInfo", explode_outer(col("ArrayInfo"))).groupBy("ArrayInfo").count()
```
but faster.

## Performance Test Report
Test Format : See RandomTestDataGenerator.scala
Test Data Size: 100 million records
Result Storage Format: ORC/Lucene file

ORC Write Performance:
- Duration: 53 seconds
- File Size: 3.0GB

Lucene Indexing Performance:
- Duration: 139 seconds
- Index Size: 13.9GB

Conditional Query Tests:
Condition: "array_contains(map_tags.`sports`, 'basketball')"

| Storage Format | Query Result Count | Query 1 Duration (secs) | Query 2 Duration (secs) | Query 3 Duration (secs) |
| -------------- | ----------------- | ---------------------- | ---------------------- | ---------------------- |
| ORC            | 24,996,139        | 11                     | 12                     | 15                     |
| Lucene         | 24,996,139        | 7                      | 2                      | 1                      |

Condition: "map_tags['sports'] is not null"

| Storage Format | Query Result Count | Query 1 Duration (secs) | Query 2 Duration (secs) | Query 3 Duration (secs) |
| -------------- | ----------------- | ---------------------- | ---------------------- | ---------------------- |
| ORC            | 49,950,531        | 16                     | 14                     | 12                     |
| Lucene         | 49,950,531        | 19                     | 2                      | 2                      |

Faceted Aggregation Test:

| Storage Format | Aggregation Duration (secs) |
| -------------- | -------------------------- |
| ORC            | 26                         |
| Lucene         | 8                          |

Based on the test results, the following conclusions can be drawn:

1. In the case of 100 million records, ORC demonstrates good write performance with a duration of 53 seconds and a file size of 3.0GB.
2. Lucene indexing takes longer, with a duration of 139 seconds, and results in a relatively bigger index size of 13.9GB.
3. For conditional queries, both ORC and Lucene are capable of quickly finding records that satisfy the given conditions. In terms of query performance, Lucene slightly outperforms ORC, especially in multiple query scenarios where Lucene exhibits faster response times.
4. In terms of faceted aggregation, ORC performs well with a duration of 26 seconds, while Lucene achieves the aggregation in 8 seconds.

In conclusion, the choice of data storage and query engine should be based on specific usage scenarios and requirements. ORC is suitable for storing and querying large-scale data efficiently, while Lucene is more suitable for multi-dimensional analysis and filtering scenarios.