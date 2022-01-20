package com.michelin.ns4kafka.cli;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.convert.format.MapFormat;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@ConfigurationProperties("kafkactl")
@JsonRootName(value = "kafkactl")
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class KafkactlConfig {
    @JsonIgnore
    public String version;

    @JsonIgnore
    public String configPath;

    public List<Context> contexts;

    public String currentContext;

    @JsonIgnore
    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    public Map<String, List<String>> tableFormat;

    @Getter
    @Setter
    public static class Context {
        public String name;
        public ApiContext context;

        @Getter
        @Setter
        public static class ApiContext {
            public String api;
            public String userToken;
            public String currentNamespace;
        }
    }
}
