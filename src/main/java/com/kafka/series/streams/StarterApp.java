package com.kafka.series.streams;

import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class StarterApp {

	private final String STATISTICS_AGGREGATE_STORE = "stat-agg";
	private final String TRANSACTIONS_QUEUE = "transactions";
	private final String STATISTICS_QUEUE = "transactions-output";
	
	public Topology createTopology(Serde<JsonNode> jsonSerde){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> transactions = builder.stream(TRANSACTIONS_QUEUE, Consumed.with(Serdes.String(), jsonSerde));
        
        ObjectNode initialStatistics = JsonNodeFactory.instance.objectNode();
        initialStatistics.put("sum", 0.0);
        initialStatistics.put("avg", 0.0);
        initialStatistics.put("max", 0.0);
        initialStatistics.put("min", 0.0);
        initialStatistics.put("count", 0);
        initialStatistics.put("time", Instant.ofEpochMilli(0L).toString());
        
        KTable<String, JsonNode> trxs = transactions
                .groupByKey(Serialized.with(Serdes.String(), jsonSerde))
                .aggregate(
                		() -> initialStatistics,
                		(key, transaction, statistics) -> computeStatistics(transaction, statistics), Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as(STATISTICS_AGGREGATE_STORE)
                		.withKeySerde(Serdes.String())
                		.withValueSerde(jsonSerde)
            		);
        trxs.toStream().to(STATISTICS_QUEUE, Produced.with(Serdes.String(), jsonSerde));
                
        return builder.build();
    }
	
	public static void main(String[] args) {
		Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "trx-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        
        StarterApp starter = new StarterApp();
        KafkaStreams streams = new KafkaStreams(starter.createTopology(jsonSerde), config);
        // only do this in dev - not in prod
//        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println("Topology:-- " + data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
	
	private static JsonNode computeStatistics(JsonNode transaction, JsonNode statistics) {
		ObjectNode newStatistics = JsonNodeFactory.instance.objectNode();
		newStatistics.put("count", statistics.get("count").asInt() + 1);
		newStatistics.put("sum", statistics.get("sum").asDouble() + transaction.get("amount").asDouble());
		newStatistics.put("avg", (statistics.get("sum").asDouble() + transaction.get("amount").asDouble()) / (statistics.get("count").asInt() + 1));
		Double statMax = Math.max(statistics.get("max").asDouble(), transaction.get("amount").asDouble());
		Double statMin = Math.min(statistics.get("min").asDouble(), transaction.get("amount").asDouble());
		newStatistics.put("max", statMax);
		newStatistics.put("min", statMin);
		Long statisticsEpoch = Instant.parse(statistics.get("time").asText()).toEpochMilli();
		Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
		Instant newStatisticsInstant = Instant.ofEpochMilli(Math.max(statisticsEpoch, transactionEpoch)); // get the latest time of event
		newStatistics.put("time", newStatisticsInstant.toString());
		
		return newStatistics;
	}

}
