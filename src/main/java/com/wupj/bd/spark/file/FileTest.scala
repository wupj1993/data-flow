package com.wupj.bd.spark.file

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * 读写文件
  */
object FileTest {
  def main(args: Array[String]): Unit = {
    readParquet
  }

  def write(): Unit = {
    /**
      * val df = spark.read
      * .format("com.crealytics.spark.excel")
      * .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
      * .option("useHeader", "true") // Required
      * .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      * .option("inferSchema", "false") // Optional, default: false
      * .option("addColorColumns", "true") // Optional, default: false
      * .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      * .option("maxRowsInMemory", 20) // optional, default none. if set, uses a streaming reader which can help with big files
      * .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      * .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
      * .schema(myCustomSchema) // optional, default: either inferred schema, or all columns are strings
      * .load("Worktime.xlsx")
      */
    val filePath = "E:\\javaworkspace\\BigData-In-Practice-master\\ImoocLogAnalysis\\src\\main\\resources\\ipRegion.xlsx";
    val spark: SparkSession = getSparkSession("local[1]", "readExcelAndWriteParquet")
    val schema = new StructType()
      .add(StructField("name", DataTypes.StringType, true))
      .add(StructField("detail", DataTypes.StringType, true))
      .add(StructField("code", DataTypes.IntegerType, true))

    val data = spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", "false").option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .schema(schema).load(filePath).toDF()
    val l = data.count()
    println("数量=", l)
    val dis = data.distinct().count()
    println("去重=" + dis)
    data.coalesce(1).write.format("parquet")
      .option("mergeSchema", true)
      .mode(SaveMode.Overwrite).parquet("region.parquet")
  }

  def readParquet(): Unit = {
    val spark = getSparkSession("local[1]", "readParquet")
    val dataFrame = spark.read.format("parquet").parquet("hdfs://10.188.181.71:9000/BDF/out/100W0509/2019-05-17/*.parquet");
    dataFrame.printSchema()
    val l = dataFrame.count()

    println(l)
  }

  def getSparkSession(master: String, appName: String) = {
    val spark: SparkSession = SparkSession.builder().master(master).appName(appName).getOrCreate()
    spark
  }
}
