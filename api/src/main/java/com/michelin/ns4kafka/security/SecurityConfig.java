package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.security.local.LocalUser;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@ConfigurationProperties("ns4kafka.security")
public class SecurityConfig {
    private List<LocalUser> localUsers;
    private String adminGroup;
}
