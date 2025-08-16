package com.javase.kafka;

import java.io.IOException;
import java.io.InputStream;
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
import org.apache.kafka.streams.kstream.KStream;

public class Main {
	public static void main(String[] args) {
		String inputTopic = "input-topic";
		String outputTopicA = "type-a-topic";
		String outputTopicB = "type-b-topic";

		// Configure Kafka Streams
		Properties props = getKafkaStreamsProperties();

		// Define the topology
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> sourceStream = builder.stream(inputTopic);

		// Split the stream based on record type
		sourceStream.filter((key, value) -> value.contains("\"type\":\"A\"")).to(outputTopicA);
		sourceStream.filter((key, value) -> value.contains("\"type\":\"B\"")).to(outputTopicB);

		// Start the streams application
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