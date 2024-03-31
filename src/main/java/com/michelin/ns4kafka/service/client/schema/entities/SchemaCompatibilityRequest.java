package com.michelin.ns4kafka.service.client.schema.entities;

import lombok.Builder;

/**
 * Schema compatibility request.
 *
 * @param compatibility The compatibility
 */
@Builder
public record SchemaCompatibilityRequest(String compatibility) {
}
