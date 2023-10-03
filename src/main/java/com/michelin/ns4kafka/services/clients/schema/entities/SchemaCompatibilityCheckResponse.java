package com.michelin.ns4kafka.services.clients.schema.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Builder;

/**
 * Schema compatibility check response.
 *
 * @param isCompatible Whether the schema is compatible or not
 * @param messages     The list of messages
 */
@Builder
public record SchemaCompatibilityCheckResponse(@JsonProperty("is_compatible") boolean isCompatible,
                                               List<String> messages) {
}
