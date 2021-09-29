package com.michelin.ns4kafka.services.schema.registry.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Builder
public class SchemaCompatibility {
    /**
     * Is the given schema compatible with the latest one
     */
    @JsonProperty("is_compatible")
    private boolean isCompatible;

    /**
     * Constructor
     *
     * @param isCompatible Is the given schema compatible with the latest one
     */
    @JsonCreator
    public SchemaCompatibility(@JsonProperty("is_compatible") boolean isCompatible) {
        this.isCompatible = isCompatible;
    }

    /**
     * Is compatible access method
     *
     * @return isCompatible property
     */
    @JsonProperty("is_compatible")
    public boolean isCompatible() {
        return isCompatible;
    }
}
