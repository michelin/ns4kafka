package com.michelin.ns4kafka.services.clients.schema.entities;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;

@Builder
@Serdeable
public record SchemaCompatibilityRequest(String compatibility) {
}
