package com.michelin.ns4kafka.services.clients.schema.entities;

import com.michelin.ns4kafka.models.schema.Schema;
import lombok.Builder;

/**
 * Schema compatibility response.
 *
 * @param compatibilityLevel The compatibility level
 */
@Builder
public record SchemaCompatibilityResponse(Schema.Compatibility compatibilityLevel) {
}
