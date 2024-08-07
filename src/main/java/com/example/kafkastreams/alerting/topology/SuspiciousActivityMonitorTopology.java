package com.example.kafkastreams.alerting.topology;

import com.example.kafkastreams.alerting.model.ActivityStatus;
import com.example.kafkastreams.alerting.model.ActivityStatusSerde;
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

public class SuspiciousActivityMonitorTopology {

    @SuppressWarnings("FieldCanBeLocal")
    private final String inputTopicName = "json-enrichment-output";
    private final Logger logger = LoggerFactory.getLogger(JsonEnrichmentTopologyBuilder.class);

    public Topology buildTopology() {
        logger.info("Building topology...");

        var jsonDocSerde = new JsonDocSerde();
        var streamsBuilder = new StreamsBuilder();

        /* build stream from input topic */
        KStream<String, JsonDoc> stream = streamsBuilder.stream(
                inputTopicName,
                Consumed.with(Serdes.String(), jsonDocSerde)
        );

        /* aggregate activity status for each user */
        stream.map((key, jsonDoc) -> {
                    /* add keys to the records */
                    var user = (String) jsonDoc.json().get("user");
                    return new KeyValue<>(user, jsonDoc);
                })
                .groupByKey(Grouped.with(Serdes.String(), jsonDocSerde))
                .aggregate(ActivityStatus::new, // this is an initializer for aggregate object (or simply accumulator)
                        (key, jsonDoc, accumulator) -> {
                            /* set user and increment total requests count */
                            accumulator.setUser(key);
                            accumulator.incrementTotalRequestsCount();

                            /* check if permission has been denied */
                            String permissionGrant = (String) jsonDoc.json().get("permission");
                            if ("DENY".equals(permissionGrant)) {
                                accumulator.incrementDeniedRequestsCount();
                            }

                            /* calculate denied ratio */
                            double ratio = accumulator.getDeniedRequestsCount() * 1.0 / accumulator.getTotalRequestsCount();
                            accumulator.setDeniedRatio(ratio);

                            /* return the aggergate object to be stored in the state store*/
                            return accumulator;
                        },
                        /* define a state store */
                        Materialized.<String, ActivityStatus, KeyValueStore<Bytes, byte[]>>as("ActivityMonitorStore")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new ActivityStatusSerde())
                )
                .toStream()
                .filter((key, activityStatus) -> activityStatus.getDeniedRatio() > 0.6)
                .peek((key, activityStatus) -> logger.warn("ALERT: Suspicious activity detected for user {}. Denied ratio: {}", key, activityStatus.getDeniedRatio()))
                .to("suspicious-user-activity-output",  Produced.with(Serdes.String(), new ActivityStatusSerde()));
        return streamsBuilder.build();
    }
}
