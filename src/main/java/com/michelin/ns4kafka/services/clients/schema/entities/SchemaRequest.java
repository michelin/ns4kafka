package com.michelin.ns4kafka.services.clients.schema.entities;

import com.michelin.ns4kafka.models.schema.Schema;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;

import java.util.List;

@Builder
@Serdeable
public record SchemaRequest(String schemaType, String schema, List<Schema.SchemaSpec.Reference> references) {
}
