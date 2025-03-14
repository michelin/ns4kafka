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

import java.util.List;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Regex utils. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RegexUtils {
    /**
     * Convert wildcard strings list to regex patterns list.
     *
     * @param wildcardStrings The list of wildcard strings
     * @return A list of regex patterns
     */
    public static List<String> convertWildcardStringsToRegex(List<String> wildcardStrings) {
        return wildcardStrings.stream()
                .map(wildcardString -> "^"
                        + wildcardString
                                .replace(".", "\\.")
                                .replace("*", ".*")
                                .replace("?", ".")
                                .replaceAll("^$", ".*") + "$")
                .toList();
    }

    /**
     * Check if a string matches any pattern of a given list.
     *
     * @param resourceName The string
     * @param regexPatterns The regex patterns
     * @return true if any regex pattern matches the resourceName, false otherwise
     */
    public static boolean isResourceCoveredByRegex(String resourceName, List<String> regexPatterns) {
        return regexPatterns.stream()
                .anyMatch(pattern ->
                        Pattern.compile(pattern).matcher(resourceName).matches());
    }
}
