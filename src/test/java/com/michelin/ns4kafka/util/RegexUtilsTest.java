package com.michelin.ns4kafka.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Regex utils test.
 */
public class RegexUtilsTest {
    @Test
    void wildcardStringsToRegexPatterns() {
        assertEquals(List.of("^.*$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("")));
        assertEquals(List.of("^prefix.*$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("prefix*")));
        assertEquals(List.of("^.*suffix$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("*suffix")));
        assertEquals(List.of("^prefix.*suffix$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("prefix*suffix")));
        assertEquals(List.of("^abc\\..*$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("abc.*")));
        assertEquals(List.of("^item.$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("item?")));
        assertEquals(List.of("^...xyz$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("???xyz")));
        assertEquals(List.of("^.*\\.topic.$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("*.topic?")));
        assertEquals(List.of("^.*\\.topic.$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("*.topic?")));
        assertEquals(List.of("^abc.\\..*-test.$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("abc?.*-test?")));

        assertEquals(List.of("^prefix.*$", "^.*suffix$"),
            RegexUtils.wildcardStringsToRegexPatterns(List.of("prefix*", "*suffix")));
    }

    @Test
    void filterByPattern() {
        assertTrue(RegexUtils.filterByPattern("topic1", List.of("^.*$")));

        assertTrue(RegexUtils.filterByPattern("prefix.topic1", List.of("^prefix.*$")));
        assertTrue(RegexUtils.filterByPattern("prefix1.topic1", List.of("^prefix.*$")));
        assertFalse(RegexUtils.filterByPattern("topic1", List.of("^prefix.*$")));

        assertTrue(RegexUtils.filterByPattern("abc1.topic-test2", List.of("^abc.\\..*-test.$")));
        assertTrue(RegexUtils.filterByPattern("abc1.stream-test2", List.of("^abc.\\..*-test.$")));
        assertFalse(RegexUtils.filterByPattern("abc1.topic-test20", List.of("^abc.\\..*-test.$")));
        assertFalse(RegexUtils.filterByPattern("abc.topic-test2", List.of("^abc.\\..*-test.$")));
        assertFalse(RegexUtils.filterByPattern("abc1.topic-test", List.of("^abc.\\..*-test.$")));
        assertFalse(RegexUtils.filterByPattern("abc1.topic-prod2", List.of("^abc.\\..*-test.$")));

        assertTrue(RegexUtils.filterByPattern("prefix1.topic", List.of("^prefix1.*$", "^prefix2.*$")));
        assertTrue(RegexUtils.filterByPattern("prefix2.topic", List.of("^prefix1.*$", "^prefix2.*$")));
        assertFalse(RegexUtils.filterByPattern("prefix3.topic", List.of("^prefix1.*$", "^prefix2.*$")));
        assertFalse(RegexUtils.filterByPattern("topic", List.of("^prefix1.*$", "^prefix2.*$")));
    }

    /**
     * Functional tests of the wildcard filter.
     */
    @Test
    void filterByWildcard() {
        List<String> patterns1 = RegexUtils.wildcardStringsToRegexPatterns(List.of("abc.my*"));
        assertTrue(RegexUtils.filterByPattern("abc.myTopic", patterns1));
        assertTrue(RegexUtils.filterByPattern("abc.myStream", patterns1));
        assertTrue(RegexUtils.filterByPattern("abc.myConnect.xyz", patterns1));
        assertTrue(RegexUtils.filterByPattern("abc.my", patterns1));
        assertFalse(RegexUtils.filterByPattern("abc.topic", patterns1));
        assertFalse(RegexUtils.filterByPattern("myTopic", patterns1));

        List<String> patterns2 = RegexUtils.wildcardStringsToRegexPatterns(List.of("abc.myT*op?c"));
        assertTrue(RegexUtils.filterByPattern("abc.myTopic", patterns2));
        assertTrue(RegexUtils.filterByPattern("abc.myTopicTopic", patterns2));
        assertTrue(RegexUtils.filterByPattern("abc.myTaaaaaopac", patterns2));
        assertFalse(RegexUtils.filterByPattern("abc.myTopiiic", patterns2));
        assertFalse(RegexUtils.filterByPattern("abc.yourTopic", patterns2));
        assertFalse(RegexUtils.filterByPattern("abc.myTopic.suffix", patterns2));

        List<String> patterns3 = RegexUtils.wildcardStringsToRegexPatterns(List.of("*.myTopic?"));
        assertTrue(RegexUtils.filterByPattern("abc.myTopic1", patterns3));
        assertTrue(RegexUtils.filterByPattern("prefix.myTopic2", patterns3));
        assertFalse(RegexUtils.filterByPattern("abc.myTopic", patterns3));
        assertFalse(RegexUtils.filterByPattern("abc.myTopic.suffix", patterns3));
        assertFalse(RegexUtils.filterByPattern("abc.myTopic13", patterns3));

        List<String> patterns4 = RegexUtils.wildcardStringsToRegexPatterns(List.of("***.myTopic"));
        assertTrue(RegexUtils.filterByPattern("abc.myTopic", patterns4));
        assertTrue(RegexUtils.filterByPattern("prefix.myTopic", patterns4));
        assertTrue(RegexUtils.filterByPattern(".myTopic", patterns4));
        assertFalse(RegexUtils.filterByPattern("prefix.myStream", patterns4));

        List<String> patterns5 = RegexUtils.wildcardStringsToRegexPatterns(List.of("*"));
        assertTrue(RegexUtils.filterByPattern("prefix.myTopic", patterns5));
        assertTrue(RegexUtils.filterByPattern("prefix10.yourSchema", patterns5));
        assertTrue(RegexUtils.filterByPattern("whatever.whatsoever", patterns5));
        assertTrue(RegexUtils.filterByPattern("whatever", patterns5));

        List<String> patterns6 = RegexUtils.wildcardStringsToRegexPatterns(List.of(""));
        assertTrue(RegexUtils.filterByPattern("prefix.myTopic", patterns6));
        assertTrue(RegexUtils.filterByPattern("prefix10.yourSchema", patterns6));
        assertTrue(RegexUtils.filterByPattern("whatever.whatsoever", patterns6));
        assertTrue(RegexUtils.filterByPattern("whatever", patterns6));
    }
}
