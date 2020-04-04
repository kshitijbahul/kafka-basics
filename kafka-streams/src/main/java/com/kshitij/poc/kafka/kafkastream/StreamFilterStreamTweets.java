package com.kshitij.poc.kafka.kafkastream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamFilterStreamTweets {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(StreamFilterStreamTweets.class.getName());
        //Create Props
        Properties properties=new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
        // Create Topology
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        //input topic
        KStream<String,String> inputTopic=streamsBuilder.stream("corona_tweets_1");
        KStream<String,String> filteredTweets=inputTopic.filter(
                (key,jsonTweets)->extractFollowersInTweet(jsonTweets) >10000
            //extract user from Tweet

        );
        filteredTweets.to("important_tweets");
        //Build topology
        KafkaStreams kafkaStreams =new KafkaStreams(streamsBuilder.build(),properties);
        //Start Stream applications
        kafkaStreams.start();

    }

    private static Integer extractFollowersInTweet(String jsonTweets) {
        return 10001;
    }

}
