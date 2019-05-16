package com.wupj.bd.spark

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import collection.JavaConverters._

object KuduTest {
  def main(args: Array[String]): Unit = {
    //    tableResult()
    count
  }

  def count(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.read.options(Map(
      "kudu.master" -> "10.188.181.73:7051",
      "kudu.table" -> "YYTERP_SCC_SALEINFO"
    )).format("kudu")
      .load()
    df.createOrReplaceTempView("YYTERP_SCC_SALEINFO")
    df.sqlContext.sql("select count(1) from YYTERP_SCC_SALEINFO " ).show()
    spark.stop()
  }

  def tableResult(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.read.options(Map(
      "kudu.master" -> "10.188.181.73:7051",
      "kudu.table" -> "VINSTITUTIONCODE_CYEAR"
    )).format("kudu")
      .load()
    df.createOrReplaceTempView("VINSTITUTIONCODE_CYEAR")
    df.cache()
    df.sqlContext.sql("select * from VINSTITUTIONCODE_CYEAR").show(10, 300)
    df.schema.printTreeString()
    df.sqlContext.sql("select count(1)  from VINSTITUTIONCODE_CYEAR").show()
    spark.stop()
  }

  def kudu(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.read.options(Map(
      "kudu.master" -> "10.188.181.73:7051",
      "kudu.table" -> "YYTERP_SCC_SALEINFO"
    ))
      .format("kudu")
      .load()
  //  df.select("NNUM", "CYEAR", "VINSTITUTIONCODE").filter("NNUM > 300").show(20)
    // 使用spark sql进行统计
    df.createOrReplaceTempView("YYTERP_SCC_SALEINFO")
    spark.sqlContext.sql("select count(1) from YYTERP_SCC_SALEINFO").show()
    /*    val result = spark.sqlContext.sql("select CYEAR,VINSTITUTIONCODE, SUM(NNUM) as TOTAL_NUM,SUM(NMNY) as NMNY from YYTERP_SCC_SALEINFO group by VINSTITUTIONCODE,CYEAR")
    val kuduContext = new KuduContext("uf003:7051", spark.sparkContext)
    val exists = kuduContext.tableExists("VINSTITUTIONCODE_CYEAR")
    if (exists) {
      kuduContext.deleteTable("VINSTITUTIONCODE_CYEAR")
    }
    kuduContext.createTable("VINSTITUTIONCODE_CYEAR",
      result.schema, Seq("CYEAR", "VINSTITUTIONCODE")
      , new CreateTableOptions().setNumReplicas(1).addHashPartitions(List("CYEAR", "VINSTITUTIONCODE").asJava, 3)
    )
    kuduContext.insertRows(result, "VINSTITUTIONCODE_CYEAR")*/
    spark.stop()
    /*// Delete data
    kuduContext.deleteRows(filteredDF, "test_table")

    // Upsert data
    kuduContext.upsertRows(df, "test_table")

    // Update data
    val alteredDF = df.select("id", $"count" + 1)*/

    /*    df.write
          .options(Map("kudu.master"-> "kudu.master:7051", "kudu.table"-> "test_table"))
          .mode("append")
          .format("kudu").save*/
  }

}
