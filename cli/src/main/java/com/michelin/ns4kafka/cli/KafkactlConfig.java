package com.michelin.ns4kafka.cli;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.annotation.Introspected;
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
    @Introspected
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
        @Introspected
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
