package com.michelin.ns4kafka.services.schema.client.entities;

import com.michelin.ns4kafka.models.Schema;
import lombok.*;

import java.util.List;

@Getter
@Builder
public class SchemaRequest {
    private String schema;
    private List<Schema.SchemaSpec.Reference> references;
}
