package com.michelin.ns4kafka.cli.configproperties;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

@ConfigurationProperties("user")
@Getter
@Setter
public class UserConfiguration {
    private String name;
    private String token;
}
