package com.kshitij.pocs.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBack {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ProducerWithCallBack.class);
        System.out.println("Hello World");
        //create a producer properties
        Properties properties= new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create producer
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(properties);
        //Producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","Hello For code");

        //send data --this is to send async
        kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                //executes everytime a record is successfully sent
                if (exception==null){
                    logger.info("Recieved new Metadata \n"+
                            "Topic:"+metadata.topic()+"\n"+
                            "Partition:"+metadata.partition()+"\n"+
                            "Offset:"+metadata.offset()+"\n"+
                            "timestamp:"+metadata.timestamp()
                            );
                }else{
                    logger.error("error in message",exception);
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
