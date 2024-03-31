package com.michelin.ns4kafka.property;

import com.michelin.ns4kafka.security.auth.local.LocalUser;
import io.micronaut.context.annotation.ConfigurationProperties;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * Security properties.
 */
@Getter
@Setter
@ConfigurationProperties("ns4kafka.security")
public class SecurityProperties {
    private List<LocalUser> localUsers;
    private String adminGroup;
    private String aes256EncryptionKey;
}
