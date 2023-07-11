package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

@Builder
public record SchemaCompatibilityRequest(String compatibility) {
}
