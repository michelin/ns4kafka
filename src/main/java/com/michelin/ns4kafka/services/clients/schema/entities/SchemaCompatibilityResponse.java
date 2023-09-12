package com.michelin.ns4kafka.services.clients.schema.entities;

import com.michelin.ns4kafka.models.schema.Schema;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;

@Builder
@Serdeable
public record SchemaCompatibilityResponse(Schema.Compatibility compatibilityLevel) {
}
