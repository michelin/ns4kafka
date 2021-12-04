package com.michelin.ns4kafka.services.schema.client.entities;

import lombok.*;

@Getter
@Builder
public class SchemaCompatibilityRequest {
    private String compatibility;
}
