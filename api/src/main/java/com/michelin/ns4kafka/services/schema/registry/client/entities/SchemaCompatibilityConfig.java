package com.michelin.ns4kafka.services.schema.registry.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.models.Schema;
import lombok.Builder;

@Builder
public class SchemaCompatibilityConfig {
    /**
     * Compatibility to set
     */
    @JsonProperty
    private Schema.SchemaCompatibility compatibility;

    /**
     * Constructor
     *
     * @param compatibilityLevel Compatibility to set
     */
    @JsonCreator
    public SchemaCompatibilityConfig(@JsonProperty Schema.SchemaCompatibility compatibility) {
        this.compatibility = compatibility;
    }
}
