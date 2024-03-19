package com.michelin.ns4kafka.security.auth;

import com.michelin.ns4kafka.models.RoleBinding;
import java.util.List;
import lombok.Builder;
import lombok.Data;

/**
 * Authentication JWT group.
 */
@Data
@Builder
public class JwtRoleBinding {
    private String namespace;
    private List<RoleBinding.Verb> verbs;
    private List<String> resources;
}
