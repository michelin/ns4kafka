package com.michelin.ns4kafka.services.schema.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SchemaCompatibilityCheckResponse {
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
    public SchemaCompatibilityCheckResponse(@JsonProperty("is_compatible") boolean isCompatible) {
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
