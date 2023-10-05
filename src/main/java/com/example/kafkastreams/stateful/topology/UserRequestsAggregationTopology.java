package com.example.kafkastreams.stateful.topology;

import com.example.kafkastreams.model.JsonDoc;
import com.example.kafkastreams.model.JsonDocSerde;
import com.example.kafkastreams.stateless.topology.JsonEnrichmentTopologyBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserRequestsAggregationTopology {

    @SuppressWarnings("FieldCanBeLocal")
    private final String inputTopicName = "json-enrichment-output";
    private final Logger logger = LoggerFactory.getLogger(JsonEnrichmentTopologyBuilder.class);

    public Topology buildTopology() {
        logger.info("Building topology...");

        var jsonDocSerde = new JsonDocSerde();
        var streamsBuilder = new StreamsBuilder();

        KStream<String, JsonDoc> stream = streamsBuilder.stream(
                inputTopicName,
                Consumed.with(Serdes.String(), jsonDocSerde)
        );

        stream.map((key, jsonDoc) -> {
                    var user = (String) jsonDoc.json().get("user");
                    return new KeyValue<>(user, jsonDoc);
                })
                .groupByKey(Grouped.with(Serdes.String(), jsonDocSerde))
                .count(Named.as("UserRequestsAggregation"),
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("UserRequestsCountStore")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                );
//                .toStream()
//                .to("user-requests-counter-output",  Produced.with(Serdes.String(), Serdes.Long()));
        return streamsBuilder.build();
    }
}
