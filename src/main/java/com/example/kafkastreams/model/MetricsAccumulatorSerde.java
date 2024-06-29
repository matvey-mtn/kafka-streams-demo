package com.example.kafkastreams.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class MetricsAccumulatorSerde implements Serde<MetricAccumulator> {

    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Serializer<MetricAccumulator> serializer() {
        return this::serialize;
    }

    @Override
    public Deserializer<MetricAccumulator> deserializer() {
        return this::deserialize;
    }

    private byte[] serialize(String topic, MetricAccumulator accumulator) {
        if (accumulator == null) {
            return new byte[0];
        }

        try {
            return mapper.writeValueAsBytes(accumulator);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private MetricAccumulator deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, MetricAccumulator.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
