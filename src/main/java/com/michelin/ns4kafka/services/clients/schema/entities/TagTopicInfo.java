package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

@Builder
public record TagTopicInfo(String entityName, String entityType, String typeName, String entityStatus) {

    @Override
    public String toString() {
        return entityName + "/" + typeName;
    }

}
