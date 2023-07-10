package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

@Builder
public record SchemaResponse(Integer id, Integer version, String subject, String schema, String schemaType) {
}
