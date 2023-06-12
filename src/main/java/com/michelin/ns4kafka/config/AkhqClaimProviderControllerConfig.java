package com.michelin.ns4kafka.config;

import com.michelin.ns4kafka.models.AccessControlEntry;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@ConfigurationProperties("ns4kafka.akhq")
public class AkhqClaimProviderControllerConfig {
    private String groupLabel;
    private List<String> roles;
    private Map<AccessControlEntry.ResourceType, String> newRoles;

    private String adminGroup;
    private List<String> adminRoles;
    private Map<AccessControlEntry.ResourceType, String> newAdminRoles;
}
