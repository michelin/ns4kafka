package com.michelin.ns4kafka.services.clients.connect.entities;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;

import java.util.Map;

@Builder
public record ConnectorSpecs(@JsonAnyGetter @JsonInclude(value = JsonInclude.Include.NON_ABSENT) Map<String, String> config) {
}
