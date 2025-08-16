package com.javase.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Main {
	public static void main(String[] args) {
		Properties props = getKafkaStreamsProperties();

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

		// Split into separate topics - Approach 1: Using branch() method
		KStream<String, String>[] branches = inputStream.branch((key, value) -> value.contains("\"channel\":\"sms\""),
				(key, value) -> value.contains("\"channel\":\"email\""));
		branches[0].to("sms-topic");
		branches[1].to("email-topic");

		// Split into separate topics - Approach 2: Using split() method
		Map<String, KStream<String, String>> branchedStreams = inputStream.split()
				.branch((key, value) -> value.contains("\"channel\":\"sms\""), Branched.as("SMS"))
				.branch((key, value) -> value.contains("\"channel\":\"email\""), Branched.as("Email")).defaultBranch();

		KStream<String, String> branchA = branchedStreams.get("SMS");
		KStream<String, String> branchB = branchedStreams.get("Email");

		branchA.to("sms-topic");
		branchB.to("email-topic");

		// Split into separate topics - Approach 3: Using filter() method
		inputStream.filter((key, value) -> value.contains("\"channel\":\"sms\"")).to("sms-topic");
		inputStream.filter((key, value) -> value.contains("\"channel\":\"email\"")).to("email-topic");

		// Start the Kafka Streams application
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();

		// Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	private static Properties getKafkaStreamsProperties() {
		Properties systemProp = loadKafkaProperties();

		Properties props = new Properties();
		props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, systemProp.getProperty("kafka.bootstrap.servers"));
		props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, systemProp.getProperty("kafka.application.id"));
		props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, systemProp.getProperty("kafka.group.id"));
		props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
				systemProp.getProperty("kafka.security.protocol"));
		props.putIfAbsent(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
		props.putIfAbsent(SaslConfigs.SASL_MECHANISM, systemProp.getProperty("kafka.sasl.mechanism"));
		props.putIfAbsent(SaslConfigs.SASL_JAAS_CONFIG, systemProp.getProperty("kafka.sasl.jaas.config"));
		props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
		props.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

		return props;
	}

	private static Properties loadKafkaProperties() {
		Properties props = new Properties();
		try {
			InputStream input = Main.class.getClassLoader().getResourceAsStream("kafka.properties");
			props.load(input);
		} catch (IOException ex) {
			System.out.println("Exception occurred: " + ex.getMessage());
		}
		return props;
	}
}