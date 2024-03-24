package com.michelin.ns4kafka.security.auth;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * JWT field constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JwtField {
    public static final String ROLES = "roles";
    public static final String ROLE_BINDINGS = "roleBindings";
    public static final String SUB = "sub";

    /**
     * JWT role binding field constants.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class JwtRoleBindingField {
        public static final String NAMESPACE = "namespace";
        public static final String VERBS = "verbs";
        public static final String RESOURCE_TYPES = "resourceTypes";
    }
}
