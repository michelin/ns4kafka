package com.michelin.ns4kafka.service.client.schema.entities;

import lombok.Builder;

/**
 * Tag name.
 *
 * @param name Tag name
 */
@Builder
public record TagInfo(String name) {
}