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
public class KafkactlConfig {
    /**
     * Configuration version
     */
    public String version;

    /**
     * Configuration file path
     */
    public String configPath;

    /**
     * Current API
     */
    String api;

    /**
     * Current user token
     */
    String userToken;

    /**
     * Current namespace
     */
    String currentNamespace;

    /**
     * List of available contexts
     */
    List<Context> contexts;

    /**
     * All table formats
     */
    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    public Map<String, List<String>> tableFormat;

    @Getter
    @Setter
    public static class Context {
        /**
         * Context name
         */
        String name;

        /**
         * Context information
         */
        ApiContext context;

        @Getter
        @Setter
        public static class ApiContext {
            /**
             * Context API
             */
            String api;

            /**
             * Context user token
             */
            String userToken;

            /**
             * Context namespace
             */
            String namespace;
        }
    }
}
