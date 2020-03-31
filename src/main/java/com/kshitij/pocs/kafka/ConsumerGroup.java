package com.kshitij.pocs.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroup {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerGroup.class);
        String groupId="my-fourth-application";
        String topicName="first_topic";
        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create a consumer
        KafkaConsumer<String,String> kafkaConsumer= new KafkaConsumer<String,String>(properties);
        kafkaConsumer.subscribe(Collections.singleton(topicName));
        while(true){
           ConsumerRecords<String,String> records=kafkaConsumer.poll(Duration.ofMillis(1000));
            records.iterator().forEachRemaining((record)->{
                logger.info("Key :"+record.key()+"\n"+
                        "Value:"+record.value()+"\n"+
                        "Partitions:"+record.partition()+"\n"+
                        "Offset:"+record.offset()+"\n"
                );
            });
        }
    }


}
