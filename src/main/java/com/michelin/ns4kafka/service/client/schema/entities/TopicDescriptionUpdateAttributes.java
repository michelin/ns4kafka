package com.michelin.ns4kafka.service.client.schema.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;

/**
 * Topic description update body's entity's information.
 *
 * @param qualifiedName topic entity name
 * @param description topic description
 */
@Builder
@JsonInclude
public record TopicDescriptionUpdateAttributes(String qualifiedName, String description) {

}
