package com.michelin.ns4kafka.services.schema.client.entities;

import com.michelin.ns4kafka.models.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaRequest {
    private String schema;
    private List<Schema.SchemaSpec.Reference> references;
}
