package com.michelin.ns4kafka.utils.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Connector configuration.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConnectorConfig {
    public static final String CONNECTOR_CLASS = "connector.class";
}
