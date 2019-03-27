package com.wupj.bd.spark

import java.util.Properties

import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}


object SparkJob {
  def main(args: Array[String]): Unit = {
    metricJob()
  }

  def metricJob(): Unit = {
    val session = getSparkSession("YYTERP_SCC_SALEINFO_TEST_METRIC", "spark://uf001:7077")
    var data = session.sqlContext.read.format("org.apache.phoenix.spark")
      .option("table", "YYTERP_SCC_SALEINFO_TEST").option("zkUrl", "uf001:2181")
      .load();
    val properties = getMysqlProperties()
    data.createOrReplaceTempView("YYTERP_SCC_SALEINFO_TEST_VIEW")
    session.sqlContext.sql("select CINVENTORYCODE ,sum(NNUM) TOTAL_NUM,sum(NMNY) as TOTAL_MNY from YYTERP_SCC_SALEINFO_TEST_VIEW group by CINVENTORYCODE")
      .toDF("CINVENTORYCODE", "TOTAL_NUM", "TOTAL_MNY")
      .write.mode(SaveMode.Append)
      .jdbc(properties.getProperty("jdbcUrl"), "VINSTITUTIONCODE_TEST_METRIC", properties)
    println("计算结束")
  }

  def getMysqlProperties(): Properties = {
    val jdbcHostname = "uf002"
    val jdbcPort = 3306
    val jdbcDatabase = "sodog"
    val user = "root"
    val password = "YYTdb@2019"
    val table = "metric"
    val driver = "com.mysql.jdbc.Driver"
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?useSSL=false"
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${user}")
    connectionProperties.put("password", s"${password}")
    connectionProperties.put("jdbcHostname", s"${jdbcHostname}")
    connectionProperties.put("jdbcPort", s"${jdbcPort}")
    connectionProperties.put("jdbcUrl", s"${jdbcUrl}")
    connectionProperties.put("table", s"${table}")
    connectionProperties.put("driver", s"${driver}")
    return connectionProperties;
  }

  def getSparkSession(appName: String, master: String): SparkSession = {
    return SparkSession.builder().master(master).appName(appName)
      .config("fs.hdfs.impl", classOf[DistributedFileSystem].getName).getOrCreate();
  }
}
