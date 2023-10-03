package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

@Builder
public record TagSpecs(String entityName, String entityType, String typeName) {

    @Override
    public String toString() {
        return entityName + "/" + typeName;
    }
    
}
