package com.example.kafkastreams.stateless;

public record EnrichmentDefinition(String fieldName, String fieldValue, EnrichmentType type) {
}