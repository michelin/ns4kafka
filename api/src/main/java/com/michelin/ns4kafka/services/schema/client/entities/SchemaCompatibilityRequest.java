package com.michelin.ns4kafka.services.schema.client.entities;

import lombok.*;

@Getter
@Setter
@Builder
public class SchemaCompatibilityRequest {
    private String compatibility;
}
