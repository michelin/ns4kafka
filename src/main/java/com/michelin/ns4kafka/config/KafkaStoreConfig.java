package com.michelin.ns4kafka.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.convert.format.MapFormat;

import java.util.Map;

@ConfigurationProperties("ns4kafka.store.kafka.topics")
public class KafkaStoreConfig {

    String prefix;
    private int replicationFactor;

    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    Map<String, String> props;

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public Map<String, String> getProperties() {
        return props;
    }

    public void setProperties(Map<String, String> properties) {
        this.props = properties;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }
}
