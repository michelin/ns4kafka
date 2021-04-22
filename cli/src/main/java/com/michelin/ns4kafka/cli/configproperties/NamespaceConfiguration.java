package com.michelin.ns4kafka.cli.configproperties;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties("namespace")
@Getter
@Setter
public class NamespaceConfiguration {
    private String name;
    private String path;
}
