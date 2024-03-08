package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

import java.util.Optional;

/**
 * Attribute entities.
 *
 * @param qualifiedName topic entity name
 * @param description topic description if any
 * @param name topic name
 */
@Builder
public record TopicListResponseEntityAttributes(String qualifiedName, Optional<String> description, String name) {

}
