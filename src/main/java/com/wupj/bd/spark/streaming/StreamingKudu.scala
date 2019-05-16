package com.wupj.bd.spark

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object StreamingKudu {
  val userInfoSchema = StructType(
    //         col name   type     nullable?
    StructField("CYEAR", StringType, false) ::
      StructField("CMONTH", StringType, true) ::
      StructField("DSALEDATE", StringType, true) ::
      StructField("VINSTITUTIONCODE", StringType, true) ::
      StructField("PK_SCC_SALEINFO", StringType, true) ::
      StructField("CCUSTOMERCODE", StringType, true) ::
      StructField("CINVENTORYCODE", StringType, true) ::
      StructField("VBRAND", StringType, true) ::
      StructField("VINVLEVEL", StringType, true) ::
      StructField("VCITYCODE", StringType, true) ::
      StructField("VPROVINCECODE", StringType, true) ::
      StructField("TS", StringType, true) ::
      StructField("NNUM", IntegerType, true) ::
      StructField("NMNY", IntegerType, true) ::
      StructField("CREATE_TS", DateType, true) ::
      StructField("DR", IntegerType, true) :: Nil
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Kafka2Spark").master("local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "10.188.181.71:9092"
//      , "auto.offset.reset" -> "earliest" // 从头开始消费
      , "auto.offset.reset" -> "latest"
 //      , "security.protocol" -> "SASL_PLAINTEXT"
//           , "sasl.kerberos.service.name" -> "kafka"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> "spark-kafka"
    )

    val dStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String]("YYT_TEST".split(",").toSet, kafkaParams))
    //  val kuduContext = new KuduContext("10.188.181.73:7051", spark.sparkContext)
    dStream.foreachRDD(rdd => {
      rdd.map(line => {
        println(line.key() + ":" + line.value())
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
