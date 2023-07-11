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
    private Map<AccessControlEntry.ResourceType, String> roles;
    private List<String> formerRoles;

    private String adminGroup;
    private Map<AccessControlEntry.ResourceType, String> adminRoles;
    private List<String> formerAdminRoles;
}
