package com.michelin.ns4kafka.security.auth;

import com.michelin.ns4kafka.model.RoleBinding;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Authentication role binding.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuthenticationRoleBinding {
    private List<String> namespaces;
    private List<RoleBinding.Verb> verbs;
    private List<String> resourceTypes;
}
