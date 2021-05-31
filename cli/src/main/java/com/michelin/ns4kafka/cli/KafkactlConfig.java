package com.michelin.ns4kafka.cli;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties("kafkactl")
@Getter
@Setter
public class KafkactlConfig {

    public String api;
    public String userToken;
    public String currentNamespace;
}
