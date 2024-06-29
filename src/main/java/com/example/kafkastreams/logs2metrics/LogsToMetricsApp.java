package com.example.kafkastreams.logs2metrics;

import com.example.kafkastreams.model.JsonDocSerde;
import com.example.kafkastreams.model.MetricAccumulator;
import com.example.kafkastreams.model.MetricsAccumulatorSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class LogsToMetricsApp {

    private static final Logger logger = LoggerFactory.getLogger(LogsToMetricsApp.class);
    private static final String INPUT_TOPIC_NAME = "json-enrichment";
    private static final String OUTPUT_TOPIC_NAME = "metric-aggregations";
    private static final String AGGREGATION_FIELD = "requestedResourceId";

    @SuppressWarnings("resource")
    public static void main(String[] args) {

        var streamsBuilder = new StreamsBuilder();

        streamsBuilder.stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.String(), new JsonDocSerde()))
                .map((key, jsonDoc) -> {
                    var user = (String) jsonDoc.json().get("user");
                    return new KeyValue<>(user, jsonDoc);
                })
                .groupByKey(Grouped.with(Serdes.String(), new JsonDocSerde()))
                .aggregate(
                        MetricAccumulator::new, (key, json, accumulator) -> {
                            int aggregatedField = (Integer) json.json().get(AGGREGATION_FIELD);
                            int count = accumulator.getCount();
                            int sum = accumulator.getSum();

                            count++;
                            sum += aggregatedField;
                            var average = sum * 1.0 / count;

                            accumulator.setCount(count);
                            accumulator.setSum(sum);
                            accumulator.setAverage(average);
                            return accumulator;
                        },
                        Named.as("l2m-aggregator"),
                        Materialized.with(Serdes.String(), new MetricsAccumulatorSerde())
                )
                .toStream()
                .to(OUTPUT_TOPIC_NAME, Produced.valueSerde(new MetricsAccumulatorSerde()));

        var topology = streamsBuilder.build();

        logger.info("{}", topology.describe());
        var kafkaStreams = new KafkaStreams(topology, streamsConfig());
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static Properties streamsConfig() {
        var properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "logs-to-metrics-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "logs-to-metrics-app");


        /* Producer Configs */
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        return properties;
    }
}
