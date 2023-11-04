package com.example.kafkastreams.stateless.enrichments;

import com.example.kafkastreams.model.JsonDoc;

import java.util.List;

public class JsonEnrichmentProcessor {

    private final List<EnrichmentDefinition> enrichments;

    public JsonEnrichmentProcessor(List<EnrichmentDefinition> enrichments) {
        this.enrichments = enrichments;
    }

    public JsonDoc process(JsonDoc jsonDoc) {
        var json = jsonDoc.json();

        for (EnrichmentDefinition enrichmentDef : enrichments) {
            switch (enrichmentDef.type()) {
                case ADD_FIELD -> json.put(
                        enrichmentDef.fieldName(), enrichmentDef.fieldValue()
                );
                case REMOVE_FIELD -> json.remove(enrichmentDef.fieldName());
                case TO_LOWER_CASE -> json.put(enrichmentDef.fieldName(), json.get(enrichmentDef.fieldName()).toString().toLowerCase());
                case TO_UPPER_CASE ->
                        json.put(enrichmentDef.fieldName(), json.get(enrichmentDef.fieldName()).toString().toUpperCase());
                case CONVERT_TO_INTEGER -> json.put(enrichmentDef.fieldName(), Integer.parseInt(json.get(enrichmentDef.fieldName()).toString()));
                default -> throw new IllegalStateException("Unexpected value: " + enrichmentDef.type());
            }
        }

        return jsonDoc;
    }

}
