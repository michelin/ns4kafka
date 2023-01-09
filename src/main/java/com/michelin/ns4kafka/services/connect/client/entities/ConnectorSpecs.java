package com.michelin.ns4kafka.services.connect.client.entities;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;

import java.util.Map;

@Builder
public class ConnectorSpecs {
    /**
     * Connector configuration
     */
    private final Map<String,String> config;

    /**
     * Getter for config
     *
     * @return The config attribute
     */
    @JsonAnyGetter
    @JsonInclude(value = JsonInclude.Include.NON_ABSENT)
    public Map<String, String> config() {
        return config;
    }
}
