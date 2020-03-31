package com.kshitij.pocs.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithTread {
    public static void main(String[] args) {
        new ConsumerWithTread().run();
    }
    private ConsumerWithTread(){

    }

    private void run(){
            Logger logger = LoggerFactory.getLogger(ConsumerWithTread.class);
            String groupId="my-sixth-application";
            String topicName="first_topic";
            String bootStrapServer="localhost:9092";
            CountDownLatch latch=new CountDownLatch(1);
            logger.info("Creating consumer thread");
            Runnable consumerThread = new ConsumerThread(latch,topicName,groupId,bootStrapServer);

            Thread myThread=new Thread(consumerThread);
            myThread.start();
            Runtime.getRuntime().addShutdownHook(new Thread(()->{
                logger.info("Caught shutdown hook");
                ((ConsumerThread) consumerThread).shutdown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }));
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Error",e);
            }finally {
                logger.info("Application is Closing");
            }
        }

    public class ConsumerThread implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String,String> kafkaConsumer;
        private Logger logger=LoggerFactory.getLogger(ConsumerThread.class);
        public ConsumerThread(CountDownLatch latch,String topic,String groupId,String bootStrapServer){
            this.latch=latch;
            Properties properties= new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            this.kafkaConsumer= new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            try{
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
            }catch (WakeupException wue){
                logger.info("recieved shutdown exception");
            }finally {
                kafkaConsumer.close();
                //tell main code we are down with shutdown
                latch.countDown();
            }

        }
        public void shutdown(){
            //This method will interupt consumer.poll
            //It will Throw wakeup exception
            logger.info("In the shutdown method");
            kafkaConsumer.wakeup();
        }


    }
}



