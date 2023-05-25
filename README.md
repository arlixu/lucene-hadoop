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
