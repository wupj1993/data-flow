package com.wupj.bd.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;


/**
 * 〈日志生成工具〉
 *
 * @author wupeiji
 * @date 2019/5/13 15:00
 * @since 1.0.0
 */
public class UbaLogProducer {

    private static final String TOPIC_NAME = "order";

    public static void main(String[] args) {
        new UbaLogProducer().produceMsg();
    }

    private void produceMsg() {
        KafkaProducer kafkaProducer = getKafkaProducer();
        for (int i = 0; i < 1000; i++) {
            String key = "keyNEW-" + i;
            String message = "WUPJ-MessageNEW-" + i;
            JSONObject jsonObject=new JSONObject();
            jsonObject.put("msg",message);
            ProducerRecord record = new ProducerRecord(TOPIC_NAME, key, jsonObject.toJSONString());
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception!=null){
                        System.out.println(exception);
                    }
                    System.out.println(metadata.topic()+metadata.offset());
                }
            });
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private KafkaProducer getKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.188.181.71:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(props);

    }
}
