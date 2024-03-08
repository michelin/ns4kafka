package com.michelin.ns4kafka.services.clients.schema.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;

import java.util.Optional;

/**
 * Attribute entities.
 *
 * @param qualifiedName topic entity name
 * @param description topic description if any
 */
@Builder
@JsonInclude()
public record TopicDescriptionUpdateAttributes(String description, String qualifiedName) {

}
