package com.wupj.bd.spark

import com.alibaba.fastjson.JSON
import com.redislabs.provider.redis.{RedisContext, toRedisStreamingContext}
import com.wupj.bd.spark.redis.RedisUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


object OrderStreaming {
  //Redis配置
  val dbIndex = 0
  //每件商品总销售额
  val orderTotalKey = "app:order:total"
  //每件商品上一分钟销售额
  val oneMinTotalKey = "app:order:product"
  //总销售额
  val totalKey = "app:order:all"

  def main(args: Array[String]): Unit = {
    // 创建 StreamingContext 时间片为1秒
    val conf = new SparkConf().setMaster("local[2]").setAppName("UserClickCountStat")
    // 五秒拉一次
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka 配置
    val topics = Set("order")
    val brokers = "10.188.181.71:9092"
    /*   kafkaParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
       kafkaParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
       kafkaParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")*/
    // 创建一个 direct stream
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "test"
//      ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> "2000"
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    conf.set("spark.redis.host", "10.188.181.73").set("spark.redis.port", "6379")

    val order = kafkaStream.map(line => JSON.parseObject(line.value())).map(x => (x.getString("id"), x.getLong("price")))
      .groupByKey()
      //      .map(x=>(x._1,x._2.reduce(_+_))).print()
      .map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))
    order.foreachRDD(rdd => {
      rdd.foreachPartition(pair => {
        pair.foreach(x => {
          // 操作redis
          val pool = new RedisUtil("10.188.181.73", 6379, 60000, null)
          val redis = pool.getRedis()
          val resource = redis.getResource
          resource.select(0)
          printf("商品ID:%s 商品销售量:%s 商品销售额:%s\n", x._1, x._2, x._3)
          //每个商品销售额累加
          resource.hincrBy(orderTotalKey, x._1, x._3)
          //上一5秒钟第每个商品销售额
          resource.hset(oneMinTotalKey, x._1.toString, x._3.toString)
          //总销售额累加
          resource.incrBy(totalKey, x._3)
          pool.returnResource(redis)
        })
      })
    }
    )

    /*
        kafkaStream.foreachRDD(rdd=>{
          rdd.foreachPartition(pair=>{

            pair.foreach(x=>{
              println()
            })
          })
        })
    */
    //  val events = kafkaStream.flatMap(x=>x.topic()).print()


    //    val events = stream.flatMap(line =>JSON.parseObject(line.value())
    ssc.start()
    ssc.awaitTermination()
  }
}
