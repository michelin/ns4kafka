package com.michelin.ns4kafka.cli.client;

import io.micronaut.core.annotation.Introspected;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Introspected
@Getter
@Setter
@Builder
public class UsernameAndPasswordRequest {
    private String username;
    private String password;
}
