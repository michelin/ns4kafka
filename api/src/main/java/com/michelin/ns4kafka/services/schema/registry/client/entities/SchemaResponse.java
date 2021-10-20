package com.michelin.ns4kafka.services.schema.registry.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

@Builder
public class SchemaResponse {
    /**
     * The schema
     */
    @JsonProperty("schema")
    private String schema;

    /**
     * Constructor
     *
     * @param schema The retrieved schema
     */
    @JsonCreator
    public SchemaResponse(@JsonProperty("schema") String schema) {
        this.schema = schema;
    }

    /**
     * The schema
     *
     * @return schema The retrieved schema
     */
    @JsonProperty("schema")
    public String schema() {
        return schema;
    }
}
