package com.kafka.series.streams;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TransactionProducer {

	private static final String TRANSACTIONS_QUEUE = "transactions";
	
	public static void main(String[] args) {
		Properties config = new Properties();
		config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		config.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
		config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
		config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		
		Producer<String, String> producer = new KafkaProducer<>(config);
		
		int i = 0;
		while(true) {
			System.out.println("Producing batch: " + i++);
			try {
				producer.send(newRandomTransaction(UUID.randomUUID().toString()));
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				break;
			}
		}
		producer.close();
	}
	
	public static ProducerRecord<String, String> newRandomTransaction(String trxid) {
		ObjectNode transaction = JsonNodeFactory.instance.objectNode();
		transaction.put("amount", ThreadLocalRandom.current().nextDouble(0, 100));
		transaction.put("time", Instant.now().toString());
		return new ProducerRecord<>(TRANSACTIONS_QUEUE, trxid, transaction.toString());
	}

}
