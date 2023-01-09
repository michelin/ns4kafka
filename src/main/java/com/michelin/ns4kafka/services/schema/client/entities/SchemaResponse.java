package com.michelin.ns4kafka.services.schema.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

@Builder
public class SchemaResponse {
    /**
     * The schema ID
     */
    private final Integer id;

    /**
     * The schema version
     */
    private final Integer version;

    /**
     * The schema subject
     */
    private final String subject;

    /**
     * The schema itself
     */
    private final String schema;

    /**
     * The schema type
     */
    private final String schemaType;

    /**
     * Constructor
     *
     * @param id The schema ID
     * @param version The schema version
     * @param subject The schema subject
     * @param schema The schema itself
     * @param schemaType The schema type
     */
    @JsonCreator
    public SchemaResponse(@JsonProperty("id") Integer id, @JsonProperty("version") Integer version, @JsonProperty("subject") String subject,
                          @JsonProperty("schema") String schema, @JsonProperty("schemaType") String schemaType) {
        this.id = id;
        this.version = version;
        this.subject = subject;
        this.schema = schema;
        this.schemaType = schemaType;
    }

    /**
     * ID access method
     *
     * @return id property
     */
    public Integer id() {
        return id;
    }

    /**
     * Version access method
     *
     * @return version property
     */
    public Integer version() {
        return version;
    }

    /**
     * Subject access method
     *
     * @return subject property
     */
    public String subject() {
        return subject;
    }

    /**
     * Schema access method
     *
     * @return schema property
     */
    public String schema() {
        return schema;
    }

    /**
     * Schema type access method
     *
     * @return schema type
     */
    public String schemaType() {
        return schemaType;
    }
}
