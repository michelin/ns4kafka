package com.michelin.ns4kafka.services.clients.schema.entities;

import com.michelin.ns4kafka.models.schema.Schema;
import lombok.Builder;

@Builder
public record SchemaCompatibilityResponse(Schema.Compatibility compatibilityLevel) {
}
