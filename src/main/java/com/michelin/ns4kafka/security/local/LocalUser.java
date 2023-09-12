package com.michelin.ns4kafka.security.local;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Slf4j
@Getter
@Setter
@Builder
@Serdeable
public class LocalUser {
    String username;
    String password;
    List<String> groups;

    public boolean isValidPassword(String inputPassword) {
        log.debug("Verifying password for user {}", username);
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest(
                    inputPassword.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexString = new StringBuilder(2 * encodedhash.length);
            for (byte b : encodedhash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            log.debug("Provided password hash : {}", hexString);
            log.debug("Expected password hash : {}", password);
            return hexString.toString().equals(password);

        } catch (NoSuchAlgorithmException e) {
            log.error("NoSuchAlgorithmException", e);
            return false;
        }
    }

}
