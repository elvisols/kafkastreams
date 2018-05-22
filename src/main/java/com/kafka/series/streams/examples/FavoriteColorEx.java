package com.kafka.series.streams.examples;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class FavoriteColorEx {

	/*
	 * Count users favorite color:
	 * 	Take a comma delimited topic of userid,color
	 * 	Keep only color green, red, or blue
	 * 	Get the running count of the favorite colors overall and output this to a topic. Note user's favorite color can change
	 * E.g:
	 * 	stephane,blue
	 * 	john,green
	 * 	stephane,red
	 * 	alice,red
	 * Output:
	 * 	blue	0
	 * 	green	1
	 * 	red		2
	 */
	public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawInput = builder.stream("raw-input");
        KStream<String, String> userColorStream = rawInput
                .filter((key, value) -> value.contains(","))
                .selectKey((key, word) -> word.split(",")[0])
                .mapValues(value -> Arrays.asList(value.split(",")).get(1))
                .filter((key, value) -> Arrays.asList("green", "red", "yellow").contains(value));
        userColorStream.to("user-color-input");
        //
        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<Long> longSerde = new Serdes.LongSerde();
        KTable<String, String> rawInputt = builder.table("user-color-input");
        
        KTable<String, Long> colorCountOutput = rawInputt
        		.groupBy((user, color) -> new KeyValue<>(color, color))
        		.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));
        
       colorCountOutput.toStream().to("color-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
	
	public static void main(String[] args) {
		Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "colorcountapplication");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        FavoriteColorEx colorCountApp = new FavoriteColorEx();

        KafkaStreams streams = new KafkaStreams(colorCountApp.createTopology(), config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}
