package com.michelin.ns4kafka.security.local;

import io.micronaut.core.annotation.Introspected;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Slf4j
@Introspected
@Builder
@Getter
@Setter
public class LocalUser {
    String username;
    String password;
    List<String> groups;

    public boolean isValidPassword(String input_password) {
        log.debug("Verifying password for user " + username);
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest(
                    input_password.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexString = new StringBuilder(2 * encodedhash.length);
            for (int i = 0; i < encodedhash.length; i++) {
                String hex = Integer.toHexString(0xff & encodedhash[i]);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            log.debug("Provided password hash : " + hexString);
            log.debug("Expected password hash : " + password);
            return hexString.toString().equals(password);

        } catch (NoSuchAlgorithmException e) {
            log.error("NoSuchAlgorithmException", e);
            return false;
        }
    }

}
