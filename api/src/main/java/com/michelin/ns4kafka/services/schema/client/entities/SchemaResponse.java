package com.michelin.ns4kafka.services.schema.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SchemaResponse {
    /**
     * The schema ID
     */
    private Integer id;

    /**
     * The schema version
     */
    private Integer version;

    /**
     * The schema subject
     */
    private String subject;

    /**
     * The schema itself
     */
    private String schema;

    /**
     * Constructor
     *
     * @param id The schema ID
     * @param version The schema version
     * @param subject The schema subject
     * @param schema The schema itself
     */
    @JsonCreator
    public SchemaResponse(@JsonProperty("id") Integer id, @JsonProperty("version") Integer version, @JsonProperty("subject") String subject,
                          @JsonProperty("schema") String schema) {
        this.id = id;
        this.version = version;
        this.subject = subject;
        this.schema = schema;
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
}
