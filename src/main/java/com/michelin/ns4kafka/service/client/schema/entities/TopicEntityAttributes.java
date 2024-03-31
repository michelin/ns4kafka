package com.michelin.ns4kafka.service.client.schema.entities;

import lombok.Builder;

/**
 * Topics list response's entity's attributes.
 *
 * @param qualifiedName topic entity name
 * @param description topic description if any
 * @param name topic name
 */
@Builder
public record TopicEntityAttributes(String qualifiedName, String description, String name) {

}
