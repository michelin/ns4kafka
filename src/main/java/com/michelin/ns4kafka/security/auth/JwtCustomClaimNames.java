package com.michelin.ns4kafka.security.auth;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * JWT custom claim names.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JwtCustomClaimNames {
    public static final String ROLES = "roles";
    public static final String ROLE_BINDINGS = "roleBindings";
}
