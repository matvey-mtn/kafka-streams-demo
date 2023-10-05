package com.example.kafkastreams.alerting.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class ActivityStatusSerde implements Serde<ActivityStatus> {
    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Serializer<ActivityStatus> serializer() {
        return this::serialize;
    }

    @Override
    public Deserializer<ActivityStatus> deserializer() {
        return this::deserialize;
    }

    private byte[] serialize(String topic, ActivityStatus activityStatus) {
        if (activityStatus == null) {
            return new byte[0];
        }

        try {
            return mapper.writeValueAsBytes(activityStatus);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private ActivityStatus deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, ActivityStatus.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
