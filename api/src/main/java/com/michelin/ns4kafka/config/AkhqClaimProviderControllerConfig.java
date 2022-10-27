package com.michelin.ns4kafka.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@ConfigurationProperties("ns4kafka.akhq")
public class AkhqClaimProviderControllerConfig {
    private String groupLabel;
    private List<String> roles;
    private String adminGroup;
    private List<String> adminRoles;
}
