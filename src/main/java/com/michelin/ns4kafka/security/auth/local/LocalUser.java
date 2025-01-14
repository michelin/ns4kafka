/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.michelin.ns4kafka.security.auth.local;

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
