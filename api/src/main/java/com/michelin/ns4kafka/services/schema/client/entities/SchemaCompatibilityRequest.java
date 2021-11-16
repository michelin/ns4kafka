package com.michelin.ns4kafka.services.schema.client.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaCompatibilityRequest {
    private String compatibility;
}
