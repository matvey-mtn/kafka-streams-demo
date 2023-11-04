package com.example.kafkastreams.stateless;

import com.example.kafkastreams.stateless.enrichments.EnrichmentDefinition;
import com.example.kafkastreams.stateless.enrichments.EnrichmentType;
import com.example.kafkastreams.stateless.enrichments.JsonEnrichmentProcessor;
import com.example.kafkastreams.stateless.topology.JsonEnrichmentTopologyBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class EnrichmentStreamsApp {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentStreamsApp.class);

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        var jsonEnrichmentProcessor = new JsonEnrichmentProcessor(Arrays.asList(
                new EnrichmentDefinition("team", "ingestion", EnrichmentType.ADD_FIELD),
                new EnrichmentDefinition("department", "core", EnrichmentType.ADD_FIELD),
                new EnrichmentDefinition("permission", null, EnrichmentType.TO_UPPER_CASE),
                new EnrichmentDefinition("requestedResourceId", null, EnrichmentType.CONVERT_TO_INTEGER)
        ));

        var topology = new JsonEnrichmentTopologyBuilder(jsonEnrichmentProcessor)
                .buildTopology();

        logger.info("{}", topology.describe());
        var kafkaStreams = new KafkaStreams(topology, streamsConfig());
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static Properties streamsConfig() {
        var properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "json-enrichments-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30 * 1000);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 50 * 1024 * 1024);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        /* Consumer Configs */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30 * 1000);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "json-enrichments-app");


        /* Producer Configs */
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        return properties;
    }
}
