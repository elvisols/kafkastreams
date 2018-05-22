package com.kafka.series.streams.examples;

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

public class BankBalanceMain {

	public Topology createTopology(Serde<JsonNode> jsonSerde){
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> bankTransactions = builder.stream("bank-transactions", Consumed.with(Serdes.String(), jsonSerde));
        
        // create the initial json object for balances
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());
        
        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey(Serialized.with(Serdes.String(), jsonSerde))
                .aggregate(
                		() -> initialBalance,
                		(key, transaction, balance) -> newBalance(transaction, balance), Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                		.withKeySerde(Serdes.String())
                		.withValueSerde(jsonSerde)
            		);
        bankBalance.toStream().to("bank-transactions-output", Produced.with(Serdes.String(), jsonSerde));
                
        return builder.build();
    }
	
	public static void main(String[] args) {
		Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        
        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        
        BankBalanceMain bbm = new BankBalanceMain();
        KafkaStreams streams = new KafkaStreams(bbm.createTopology(jsonSerde), config);
        // only do this in dev - not in prod
//        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
	}
	
	private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
		// create a new balance json object
		ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
		newBalance.put("count", balance.get("count").asInt() + 1);
		newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());
		
		Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
		Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
		Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
		newBalance.put("time", newBalanceInstant.toString());
		
		return newBalance;
	}

}
