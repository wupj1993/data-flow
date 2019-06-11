package com.wupj.bd.spark

import java.util.concurrent.Executors

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

import collection.JavaConverters._

object KuduTest {
  def main(args: Array[String]): Unit = {
    //    tableResult()
//    val threadService = Executors.newFixedThreadPool(2)
    println("开始统计")
    countKudu()

    /**
      * ------------------------------------
      *  26481203    26034
      * ------------------------------------
      * 27481203     16661
      * ------------------------------------
      *
      * ------------------------------------
      */
    /*
    threadService.execute(new Runnable {
      override def run(): Unit = {
        println("启动Phoenix统计")
        countPhoenix
      }
    })
*/
    /*
        threadService.execute(new Runnable {
          override def run(): Unit = {
            println("启动Kudu统计")
            countKudu
          }
        })
    */
  }

  def countKudu(): Unit = {
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.read.options(Map(
      "kudu.master" -> "10.188.181.73:7051",
      "kudu.table" -> "YYTERP_SCC_SALEINFO"
    )).format("kudu")
      .load()
    df.createOrReplaceTempView("YYTERP_SCC_SALEINFO")
    df.sqlContext.sql("select count(1) from YYTERP_SCC_SALEINFO ").show()
    println((System.currentTimeMillis() - start))
    spark.stop()
  }

  def countPhoenix(): Unit = {
    //  457722  YYTERP_SCC_SALEINFO_LZO  1.25
    //  401458  YYTERP_SCC_SALEINFO_NO_COMPRESSION  2.91
    //  423219  YYTERP_SCC_SALEINFO   1.45
    //
    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    var data = spark.sqlContext.read.format("org.apache.phoenix.spark")
      .option("table", "YYTERP_SCC_SALEINFO_GZ").option("zkUrl", "uf001:2181")
      .load();
    data.createOrReplaceTempView("YYTERP_SCC_SALEINFO_GZ")
    data.sqlContext.sql("select count(1) from YYTERP_SCC_SALEINFO_GZ").show()
    println("Phoenix:", (System.currentTimeMillis() - start))
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
