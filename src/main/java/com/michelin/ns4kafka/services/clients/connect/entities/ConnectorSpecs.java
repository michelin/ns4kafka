package com.michelin.ns4kafka.services.clients.connect.entities;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import lombok.Builder;

/**
 * Connector specs.
 *
 * @param config Config
 */
@Builder
public record ConnectorSpecs(
    @JsonAnyGetter @JsonInclude(value = JsonInclude.Include.NON_ABSENT) Map<String, String> config) {
}
