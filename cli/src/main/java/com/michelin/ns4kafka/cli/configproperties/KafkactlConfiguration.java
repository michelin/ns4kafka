package com.michelin.ns4kafka.cli.configproperties;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties("kafkactl")
@Getter
@Setter
public class KafkactlConfiguration {

    String api;
    String userToken;
    String currentNamespace;
    boolean verbose = false;
}
