package com.michelin.ns4kafka.services.clients.schema.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

import java.util.List;

@Builder
public record SchemaCompatibilityCheckResponse(@JsonProperty("is_compatible") boolean isCompatible, List<String> messages) {
}
