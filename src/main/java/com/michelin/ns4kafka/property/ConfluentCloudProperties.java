package com.michelin.ns4kafka.property;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * Confluent Cloud properties.
 */
@Getter
@Setter
@ConfigurationProperties("ns4kafka.confluent-cloud")
public class ConfluentCloudProperties {
    private StreamCatalogProperties streamCatalog;

    /**
     * Stream Catalog properties.
     */
    @Getter
    @Setter
    @ConfigurationProperties("ns4kafka.confluent-cloud.stream-catalog")
    public static class StreamCatalogProperties {
        private int pageSize = 500;
    }

}
