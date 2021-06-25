package com.michelin.ns4kafka.cli.client;

import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.Setter;

@Introspected
@Getter
@Setter
public class UserInfoResponse {
    private boolean active;
    private String username;
    private long exp;
}
