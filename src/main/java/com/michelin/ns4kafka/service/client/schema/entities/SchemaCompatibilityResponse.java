package com.michelin.ns4kafka.service.client.schema.entities;

import com.michelin.ns4kafka.model.schema.Schema;
import lombok.Builder;

/**
 * Schema compatibility response.
 *
 * @param compatibilityLevel The compatibility level
 */
@Builder
public record SchemaCompatibilityResponse(Schema.Compatibility compatibilityLevel) {
}
