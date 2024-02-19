package com.michelin.ns4kafka.properties;

import com.michelin.ns4kafka.models.AccessControlEntry;
import io.micronaut.context.annotation.ConfigurationProperties;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * Akhq properties.
 */
@Getter
@Setter
@ConfigurationProperties("ns4kafka.akhq")
public class AkhqProperties {
    private String groupLabel;
    private Map<AccessControlEntry.ResourceType, String> roles;
    private List<String> formerRoles;

    private String adminGroup;
    private Map<AccessControlEntry.ResourceType, String> adminRoles;
    private List<String> formerAdminRoles;
}
