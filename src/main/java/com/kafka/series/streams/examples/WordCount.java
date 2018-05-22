package com.kafka.series.streams.examples;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class WordCount {

	public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream("word-count-input");
        KTable<String, Long> wordCounts = textLines
                .mapValues(textLine -> textLine.toLowerCase())
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .count(Materialized.as("Counts"));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
	
	public static void main(String[] args) {
		Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE); // This ensures the producer is idempotent

        WordCount wordCountApp = new WordCount();

        KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), config);
        streams.start();
        
        // print topology
         System.out.println(streams.toString());

        // Update:
        // print the topology every 10 seconds for learning purposes
//        while(true){
//            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
//            try {
//                Thread.sleep(5000);
//            } catch (InterruptedException e) {
//                break;
//            }
//        }

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}
