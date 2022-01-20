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
     * Get the current context infos if it exists, or null otherwise
     *
     * @return The current context
     */
    public KafkactlConfig.Context getCurrentContextInfos() {
        return kafkactlConfig.getContexts().stream()
                .filter(context -> context.getName().equals(kafkactlConfig.getCurrentContext()))
                .findFirst().orElse(null);
    }

    /**
     * Does the given context name exist
     *
     * @param name The name to check
     * @return true if it is, false otherwise
     */
    public boolean contextExists(String name) {
        return kafkactlConfig.getContexts().stream()
                .anyMatch(context -> context.getName().equals(name));
    }
}
