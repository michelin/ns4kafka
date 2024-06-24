package com.michelin.ns4kafka.property;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * Stream Catalog properties.
 */
@Getter
@Setter
@ConfigurationProperties("ns4kafka.confluent-cloud.stream-catalog")
public class StreamCatalogProperties {
    private int pageSize = 500;
}
