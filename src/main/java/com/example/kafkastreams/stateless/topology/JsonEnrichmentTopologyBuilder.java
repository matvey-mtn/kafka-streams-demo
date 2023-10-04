package com.example.kafkastreams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonEnrichmentTopologyBuilder {

    private final JsonEnrichmentProcessor jsonEnrichmentProcessor;

    @SuppressWarnings("FieldCanBeLocal")
    private final String inputTopicName = "json-enrichment";
    private final Logger logger = LoggerFactory.getLogger(JsonEnrichmentTopologyBuilder.class);

    public JsonEnrichmentTopologyBuilder(JsonEnrichmentProcessor jsonEnrichmentProcessor) {
        this.jsonEnrichmentProcessor = jsonEnrichmentProcessor;
    }

    public Topology buildTopology() {
        logger.info("Building Topology");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, JsonDoc> stream = streamsBuilder.stream(
                inputTopicName,
                Consumed.with(Serdes.String(), new JsonDocSerde())
        );

        stream.mapValues(jsonEnrichmentProcessor::enrich, Named.as("JsonEnrichmentProcessor"))
                .to("json-enrichment-output");

        return streamsBuilder.build();
    }

}
