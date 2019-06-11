package com.wupj.bd.spark.jobtest

import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.phoenix.spark.toDataFrameFunctions

object ErpJobMetric {
  /**
    * 统计YYTERP_SCC_SALEINFO 通过group by的方式
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("phoenix_data_metric").set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
      .set("spark.dynamicAllocation.executorIdleTimeout", "60")
      .set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "60")
      .set("spark.dynamicAllocation.enabled", "true")
    val spark=SparkSession.builder.config(sparkConf).getOrCreate
    val data = spark.read.format("org.apache.phoenix.spark")
      .options(Map("zkUrl" -> "10.188.181.71,10.188.181.72,10.188.181.73", "user" -> null, "password" -> null, "table" -> "YYTERP_SCC_SALEINFO")).load()
    data.createOrReplaceTempView("YYTERP_SCC_SALEINFO")
    data.printSchema()
//    val count = data.count()
//    println("总数:" + count)
    println("group by 统计" + System.currentTimeMillis())
    val df = data.sqlContext.sql("SELECT VINSTITUTIONCODE,CYEAR,CMONTH,CINVENTORYCODE,VINVLEVEL,SUM(NNUM) as TOTAL_NUM ,SUM(NMNY) as TOTAL_MNY from YYTERP_SCC_SALEINFO where CYEAR>'2016' group by VINSTITUTIONCODE,CYEAR,CMONTH,CINVENTORYCODE,VINVLEVEL")
      .toDF("VINSTITUTIONCODE", "CYEAR", "CMONTH", "CINVENTORYCODE", "VINVLEVEL", "TOTAL_NUM", "TOTAL_MNY")
    println("统计结束" + System.currentTimeMillis())
    println("保存到Phoenix" + System.currentTimeMillis())
    df.saveToPhoenix("VINSTITUTIONCODE_METRIC", zkUrl = Some("10.188.181.71,10.188.181.72,10.188.181.73"))
    println("保存结束:" + System.currentTimeMillis())
    spark.stop()
  }

}
