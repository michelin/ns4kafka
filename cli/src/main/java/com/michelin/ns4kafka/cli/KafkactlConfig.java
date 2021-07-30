package com.michelin.ns4kafka.cli;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.convert.format.MapFormat;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@ConfigurationProperties("kafkactl")
@Getter
@Setter
public class KafkactlConfig {
    public String version;
    public String configPath;
    public String api;
    public String userToken;
    public String currentNamespace;
    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    public Map<String, List<String>> tableFormat;
}
