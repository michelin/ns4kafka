package com.michelin.ns4kafka.security.local;

import io.micronaut.core.annotation.Introspected;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Local user.
 */
@Slf4j
@Getter
@Setter
@Builder
@Introspected
public class LocalUser {
    String username;
    String password;
    List<String> groups;

    /**
     * Verify if the provided password is valid for this user.
     *
     * @param inputPassword The password to verify
     * @return true if valid, false otherwise
     */
    public boolean isValidPassword(String inputPassword) {
        log.debug("Verifying password for user {}", username);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedHash = digest.digest(
                inputPassword.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexString = new StringBuilder(2 * encodedHash.length);
            for (byte hash : encodedHash) {
                String hex = Integer.toHexString(0xff & hash);
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
