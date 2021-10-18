package com.michelin.ns4kafka.services.schema.registry.client.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.models.Subject;

public class SubjectCompatibilityResponse {
    /**
     * Compatibility level
     */
    @JsonProperty("compatibilityLevel")
    private Subject.SubjectCompatibility compatibilityLevel;

    /**
     * Constructor
     *
     * @param compatibilityLevel The current compatibility level
     */
    @JsonCreator
    public SubjectCompatibilityResponse(@JsonProperty("compatibilityLevel") Subject.SubjectCompatibility compatibilityLevel) {
        this.compatibilityLevel = compatibilityLevel;
    }

    /**
     * Compatibility level access method
     *
     * @return compatibilityLevel property
     */
    @JsonProperty("compatibilityLevel")
    public Subject.SubjectCompatibility compatibilityLevel() {
        return compatibilityLevel;
    }
}
