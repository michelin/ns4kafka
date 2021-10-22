package com.michelin.ns4kafka.services.schema.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.models.Schema;

public class SchemaCompatibilityResponse {
    /**
     * Compatibility level
     */
    @JsonProperty("compatibilityLevel")
    private Schema.Compatibility compatibilityLevel;

    /**
     * Constructor
     *
     * @param compatibilityLevel The current compatibility level
     */
    @JsonCreator
    public SchemaCompatibilityResponse(@JsonProperty("compatibilityLevel") Schema.Compatibility compatibilityLevel) {
        this.compatibilityLevel = compatibilityLevel;
    }

    /**
     * Compatibility level access method
     *
     * @return compatibilityLevel property
     */
    @JsonProperty("compatibilityLevel")
    public Schema.Compatibility compatibilityLevel() {
        return compatibilityLevel;
    }
}
