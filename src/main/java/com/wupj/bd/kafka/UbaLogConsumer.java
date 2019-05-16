package com.wupj.bd.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 〈日志消费端〉
 *
 * @author wupeiji
 * @date 2019/5/13 15:13
 * @since 1.0.0
 */
public class UbaLogConsumer {
    public static void main(String[] args) {
        try {
            new UbaLogConsumer().consumerMsg();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 消费数据
     */
    private void consumerMsg() throws InterruptedException {
        KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer();
     /*   TopicPartition partition0 = new TopicPartition("MY_TEST", 1);
        List<TopicPartition> partitionList=new ArrayList<>();
        partitionList.add(partition0);
        kafkaConsumer.assign(partitionList);*/
        kafkaConsumer.subscribe(Arrays.asList("order"));

        boolean fetchMsg = true;
        while (fetchMsg) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
            System.out.println(records.isEmpty());
            records.forEach(data -> System.out.println(data.key() + ":" + data.value() + " \n offset【" + data.offset() + "】"));
            kafkaConsumer.commitSync();
            Thread.sleep(2000);
        }

    }

    private KafkaConsumer<String, String> getKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.188.181.71:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");

        return new KafkaConsumer<String, String>(props);
    }
}
