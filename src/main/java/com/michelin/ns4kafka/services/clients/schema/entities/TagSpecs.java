package com.michelin.ns4kafka.services.clients.schema.entities;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;

import java.util.Map;

@Builder
public record TagSpecs(String entityName, String entityType, String typeName) {
}
