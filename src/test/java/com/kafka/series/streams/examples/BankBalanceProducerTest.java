package com.kafka.series.streams.examples;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BankBalanceProducerTest {

	@Test
	public void testNewRandomTransaction() {
		ProducerRecord<String, String> precord = BankBalanceProducer.newRandomTransaction("luvy");
		
		assertEquals(precord.key(), "luvy");
		
		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readTree(precord.value());
			assertEquals(node.get("name").asText(), "luvy");
			assertTrue("Amount should be less than 100", node.get("amount").asInt() < 100);
		} catch(IOException e) {
			e.printStackTrace();
		}
		System.out.println(precord.value());
	}

}
