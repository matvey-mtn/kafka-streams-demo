package com.example.kafkastreams.stateless.enrichments;

public record EnrichmentDefinition(String fieldName, String fieldValue, EnrichmentType type) {
}