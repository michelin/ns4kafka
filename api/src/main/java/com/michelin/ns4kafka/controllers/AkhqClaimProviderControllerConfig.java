package com.michelin.ns4kafka.controllers;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Builder
@ConfigurationProperties("ns4kafka.akhq")
public class AkhqClaimProviderControllerConfig {
    private String groupLabel;
    private List<String> roles;
    private String adminGroup;
    private List<String> adminRoles;
}
