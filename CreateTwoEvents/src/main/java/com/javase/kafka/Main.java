package com.javase.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Main {
	public static void main(String[] args) {
		// Configure Kafka Streams
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "channel-splitter-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your broker
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// Build the topology
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> inputStream = builder.stream("input-topic"); // Replace with your input topic

		// Approach 1
		inputStream.flatMapValues(value -> {
			// Parse the record (assuming JSON format)
			// Example: {"id": 1, "channel": "Both", "content": "Hello"}
			// Replace this with your actual parsing logic
			if (value.contains("\"channel\":\"Both\"")) {
				// Create two new records
				String smsRecord = value.replace("\"channel\":\"Both\"", "\"channel\":\"sms\"");
				String emailRecord = value.replace("\"channel\":\"Both\"", "\"channel\":\"email\"");
				return java.util.Arrays.asList(smsRecord, emailRecord);
			} else {
				return java.util.Collections.singletonList(value);
			}
		}).to("output-topic"); // Replace with your output topic

		// Approach 2
		inputStream.flatMapValues(value -> {
			ObjectMapper mapper = new ObjectMapper();
			try {
				JsonNode rootNode = mapper.readTree(value);
				JsonNode channelNode = rootNode.get("channel");
				if (channelNode != null && channelNode.asText().equals("Both")) {
					// Create SMS record
					ObjectNode smsNode = mapper.createObjectNode();
					smsNode.setAll((ObjectNode) rootNode);
					smsNode.put("channel", "sms");
					String smsRecord = mapper.writeValueAsString(smsNode);
					// Create Email record
					ObjectNode emailNode = mapper.createObjectNode();
					emailNode.setAll((ObjectNode) rootNode);
					emailNode.put("channel", "email");
					String emailRecord = mapper.writeValueAsString(emailNode);
					// Return both records
					return java.util.Arrays.asList(smsRecord, emailRecord);
				} else {
					// Return the original record if channel is not "Both"
					return java.util.Collections.singletonList(value);
				}
			} catch (Exception e) {
				System.err.println("Error parsing record: " + e.getMessage());
				return java.util.Collections.emptyList();
			}
		}).to("output-topic");

		// Start the Kafka Streams application
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();

		// Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}