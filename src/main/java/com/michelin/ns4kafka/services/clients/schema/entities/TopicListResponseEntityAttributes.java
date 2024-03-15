package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

/**
 * Topics list response's entity's information.
 *
 * @param qualifiedName topic entity name
 * @param description topic description if any
 * @param name topic name
 */
@Builder
public record TopicListResponseEntityAttributes(String qualifiedName, String description, String name) {

}
