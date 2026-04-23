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
package com.michelin.ns4kafka.util;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Topic configuration utilities. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TopicConfigUtils {
    /**
     * Normalize a topic config value before comparing it.
     *
     * @param key The config key
     * @param value The config value
     * @return The normalized config value
     */
    public static String normalizeValue(String key, String value) {
        if (value == null || !CLEANUP_POLICY_CONFIG.equals(key) || !value.contains(",")) {
            return value;
        }

        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(token -> !token.isBlank())
                .sorted()
                .collect(Collectors.joining(","));
    }

    /**
     * Compare two topic config values.
     *
     * @param key The config key
     * @param left The first config value
     * @param right The second config value
     * @return true when both values are equivalent
     */
    public static boolean areEquivalent(String key, String left, String right) {
        return Objects.equals(normalizeValue(key, left), normalizeValue(key, right));
    }

    /**
     * Check whether the cleanup policy contains both delete and compact.
     *
     * @param cleanupPolicy The cleanup policy
     * @return true when the cleanup policy contains delete and compact
     */
    public static boolean hasDeleteAndCompactCleanupPolicy(String cleanupPolicy) {
        return areEquivalent(
                CLEANUP_POLICY_CONFIG, cleanupPolicy, CLEANUP_POLICY_DELETE + "," + CLEANUP_POLICY_COMPACT);
    }
}
