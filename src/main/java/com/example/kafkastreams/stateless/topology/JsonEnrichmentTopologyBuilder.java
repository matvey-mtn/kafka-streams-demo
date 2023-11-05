package com.example.kafkastreams.stateless.topology;

import com.example.kafkastreams.stateless.enrichments.JsonEnrichmentProcessor;
import com.example.kafkastreams.model.JsonDoc;
import com.example.kafkastreams.model.JsonDocSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
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
        logger.info("Building topology...");
        /* initialize streams builder */
        var streamsBuilder = new StreamsBuilder();

        /* use streams builder to create a stream */
        KStream<String, JsonDoc> stream = streamsBuilder.stream(
                inputTopicName,
                Consumed.with(Serdes.String(), new JsonDocSerde())
        );

        /* use stream to transform json documents and produce the result to output topic */
        stream.mapValues(jsonEnrichmentProcessor::process)
                .to("json-enrichment-output", Produced.valueSerde(new JsonDocSerde()));

        /* return topology */
        return streamsBuilder.build();
    }

}
