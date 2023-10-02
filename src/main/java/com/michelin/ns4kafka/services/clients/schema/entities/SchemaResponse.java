package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

/**
 * Schema response.
 *
 * @param id         The id
 * @param version    The version
 * @param subject    The subject
 * @param schema     The schema
 * @param schemaType The schema type
 */
@Builder
public record SchemaResponse(Integer id, Integer version, String subject, String schema, String schemaType) {
}
