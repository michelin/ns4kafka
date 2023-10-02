package com.michelin.ns4kafka.services.clients.schema.entities;

import com.michelin.ns4kafka.models.schema.Schema;
import java.util.List;
import lombok.Builder;

/**
 * Schema request.
 */
@Builder
public record SchemaRequest(String schemaType, String schema, List<Schema.SchemaSpec.Reference> references) {
}
