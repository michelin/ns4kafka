package com.michelin.ns4kafka.service.client.schema.entities;

import com.michelin.ns4kafka.model.schema.Schema;
import java.util.List;
import lombok.Builder;

/**
 * Schema response.
 *
 * @param id         The id
 * @param version    The version
 * @param subject    The subject
 * @param schema     The schema
 * @param schemaType The schema type
 * @param references The schema references
 */
@Builder
public record SchemaResponse(Integer id,
                             Integer version,
                             String subject,
                             String schema,
                             String schemaType,
                             List<Schema.SchemaSpec.Reference> references) {
}
