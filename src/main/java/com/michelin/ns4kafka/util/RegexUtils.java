package com.michelin.ns4kafka.util;

import java.util.List;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Regex utils.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RegexUtils {
    /**
     * Convert wildcard strings list to regex patterns list.
     *
     * @param wildcardStrings The list of wildcard strings
     * @return A list of regex patterns
     */
    public static List<String> convertWildcardStringsToRegex(List<String> wildcardStrings) {
        return wildcardStrings
            .stream()
            .map(wildcardString -> "^" + wildcardString
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
        return regexPatterns
            .stream()
            .anyMatch(pattern -> Pattern.compile(pattern).matcher(resourceName).matches());
    }
}
