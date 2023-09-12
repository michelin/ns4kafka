package com.michelin.ns4kafka.services.clients.schema.entities;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;

@Builder
@Serdeable
public record SchemaResponse(Integer id, Integer version, String subject, String schema, String schemaType) {
}
