package com.michelin.ns4kafka.services.clients.schema.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;

import java.util.List;

@Builder
@Serdeable
public record SchemaCompatibilityCheckResponse(@JsonProperty("is_compatible") boolean isCompatible, List<String> messages) {
}
