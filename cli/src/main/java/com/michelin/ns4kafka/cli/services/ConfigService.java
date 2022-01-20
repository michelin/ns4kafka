package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.KafkactlConfig;
import picocli.CommandLine;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class ConfigService {
    /**
     * The current Kafkactl configuration
     */
    private final KafkactlConfig kafkactlConfig;

    /**
     * Constructor
     *
     * @param kafkactlConfig The current Kafkactl configuration
     */
    public ConfigService(KafkactlConfig kafkactlConfig) {
        this.kafkactlConfig = kafkactlConfig;
    }

    /**
     * Get the current context if it exists, or null otherwise
     *
     * @return The current context
     */
    public KafkactlConfig.Context getCurrentContext() {
        return kafkactlConfig.getContexts().stream()
                .filter(context -> context.getName().equals(kafkactlConfig.getCurrentContext()))
                .findFirst().orElse(null);
    }
}
