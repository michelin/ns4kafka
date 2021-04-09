package com.michelin.ns4kafka.security.local;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

@Builder
@Getter
@Setter
public class LocalUser {
    private static final Logger LOG = LoggerFactory.getLogger(LocalUser.class);
    String username;
    String password;
    List<String> groups = new ArrayList<>();

    public boolean isValidPassword(String input_password) {
        LOG.debug("Verifying password for user " + username);
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
            LOG.debug("Provided password hash : " + hexString);
            LOG.debug("Expected password hash : " + password);
            return hexString.toString().equals(password);

        } catch (NoSuchAlgorithmException e) {
            LOG.error("NoSuchAlgorithmException",e);
            return false;
        }
    }

}
