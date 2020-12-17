package com.michelin.ns4kafka.services;


import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.convert.format.MapFormat;
import lombok.Getter;

import java.util.Map;

@Getter
@EachProperty("ns4kafka.managed-clusters")
public class KafkaAsyncExecutorConfig {
    private final String name;
    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    Map<String, Object> config;

    public KafkaAsyncExecutorConfig(@Parameter String name) {
        this.name = name;
    }
}
