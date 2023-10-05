package com.example.kafkastreams.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonDocSerde implements Serde<JsonDoc> {
    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Serializer<JsonDoc> serializer() {
        return this::serialize;
    }

    @Override
    public Deserializer<JsonDoc> deserializer() {
        return this::deserialize;
    }

    private byte[] serialize(String topic, JsonDoc jsonDoc) {
        if (jsonDoc == null) {
            return new byte[0];
        }

        try {
            return mapper.writeValueAsBytes(jsonDoc.json());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonDoc deserialize(String topic, byte[] data) {
        try {
            var json = mapper.readValue(data, new TypeReference<Map<String, Object>>() {});
            return new JsonDoc(json);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
