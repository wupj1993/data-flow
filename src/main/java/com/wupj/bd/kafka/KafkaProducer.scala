package com.wupj.bd.kafka


import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.util.Random

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val topic = "order"
    val brokers = "10.188.181.71:9092"
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val product = new KafkaProducer[String, String](props)
    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        println(exception)
        println(metadata.topic()+"_"+metadata.offset())
      }
    }
    while (true) {
      //随机生成10以内ID
      val id = Random.nextInt(10)
      //创建订单成交事件
      val event = new JSONObject();
      //商品ID
      event.put("id", id)
      //商品成交价格
      event.put("price", Random.nextInt(10000))

      //发送信息
      product.send(new ProducerRecord[String, String](topic, System.currentTimeMillis().toString, event.toString), callback)

      println("Message sent: " + event)
      //随机暂停一段时间
      Thread.sleep(Random.nextInt(2000))
    }
  }


}
