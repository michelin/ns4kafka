package com.michelin.ns4kafka.services.schema.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

import java.util.List;

@Builder
public class SchemaCompatibilityCheckResponse {
    /**
     * Is the given schema compatible with the latest one
     */
    private final boolean isCompatible;

    /**
     * When compatibility fails, list of reasons why
     */
    private final List<String> messages;

    /**
     * Constructor
     *
     * @param isCompatible Is the given schema compatible with the latest one
     * @param messages When compatibility fails, list of reasons why
     */
    @JsonCreator
    public SchemaCompatibilityCheckResponse(@JsonProperty("is_compatible") boolean isCompatible,
                                            @JsonProperty("messages") List<String> messages) {
        this.isCompatible = isCompatible;
        this.messages = messages;
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

    /**
     * Messages access method
     *
     * @return messages property
     */
    @JsonProperty("messages")
    public List<String> messages() {
        return messages;
    }
}
