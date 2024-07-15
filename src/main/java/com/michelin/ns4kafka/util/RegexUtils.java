package com.michelin.ns4kafka.util;

import java.util.List;
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
    public static List<String> wildcardStringsToRegexPatterns(List<String> wildcardStrings) {
        return wildcardStrings.stream()
                .map(wildcardString -> "^" + wildcardString
                        .replace(".", "\\.")
                        .replace("*", ".*")
                        .replace("?", ".")
                        .replaceAll("^$", ".*") + "$")
                .toList();
    }
}
