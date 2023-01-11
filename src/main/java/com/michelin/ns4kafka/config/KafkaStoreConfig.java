package com.michelin.ns4kafka.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.convert.format.MapFormat;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@ConfigurationProperties("ns4kafka.store.kafka.topics")
public class KafkaStoreConfig {
    private String prefix;
    private int replicationFactor;

    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    private Map<String, String> props;
}
