package com.michelin.ns4kafka.properties;

import com.michelin.ns4kafka.security.local.LocalUser;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Serdeable
@ConfigurationProperties("ns4kafka.security")
public class SecurityProperties {
    private List<LocalUser> localUsers;
    private String adminGroup;
    private String aes256EncryptionKey;
}
