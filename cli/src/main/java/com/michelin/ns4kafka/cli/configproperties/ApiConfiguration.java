package com.michelin.ns4kafka.cli.configproperties;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties("api")
@Getter
@Setter
public class ApiConfiguration {
    private String name;
    private String server;
}
