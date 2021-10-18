package com.michelin.ns4kafka.services.schema.registry.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.models.Subject;
import lombok.Builder;

@Builder
public class SubjectCompatibilityRequest {
    /**
     * Compatibility to set
     */
    @JsonProperty
    private Subject.SubjectCompatibility compatibility;

    /**
     * Constructor
     *
     * @param compatibility Compatibility to set
     */
    @JsonCreator
    public SubjectCompatibilityRequest(@JsonProperty Subject.SubjectCompatibility compatibility) {
        this.compatibility = compatibility;
    }
}
