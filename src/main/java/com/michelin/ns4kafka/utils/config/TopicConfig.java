package com.michelin.ns4kafka.utils.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic configuration.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TopicConfig {
    public static final String PARTITIONS = "partitions";
    public static final String REPLICATION_FACTOR = "replication.factor";
}
