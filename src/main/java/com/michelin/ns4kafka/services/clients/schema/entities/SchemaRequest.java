package com.michelin.ns4kafka.services.clients.schema.entities;

import com.michelin.ns4kafka.models.schema.Schema;
import lombok.Builder;

import java.util.List;

@Builder
public record SchemaRequest(String schemaType, String schema, List<Schema.SchemaSpec.Reference> references) {
}
