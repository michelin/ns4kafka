package com.michelin.ns4kafka.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Regex utils test.
 */
class RegexUtilsTest {
    @Test
    void defaultStringToRegexPatterns() {
        assertEquals(List.of("^.*$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("")));
        assertEquals(List.of("^.*$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("*")));
    }

    @Test
    void simpleWildcardToRegexPatterns() {
        assertEquals(List.of("^prefix.*$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("prefix*")));
        assertEquals(List.of("^.*suffix$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("*suffix")));
        assertEquals(List.of("^abc\\..*$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("abc.*")));
        assertEquals(List.of("^item.$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("item?")));
    }

    @Test
    void complexWildcardToRegexPatterns() {
        assertEquals(List.of("^prefix.*suffix$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("prefix*suffix")));
        assertEquals(List.of("^...xyz$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("???xyz")));
        assertEquals(List.of("^.*\\.topic.$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("*.topic?")));
        assertEquals(List.of("^.*\\.topic.$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("*.topic?")));
        assertEquals(List.of("^abc.\\..*-test.$"), RegexUtils.wildcardStringsToRegexPatterns(List.of("abc?.*-test?")));
    }

    @Test
    void multipleWildcardsToRegexPatterns() {
        assertEquals(List.of("^prefix.*$", "^.*suffix$"),
            RegexUtils.wildcardStringsToRegexPatterns(List.of("prefix*", "*suffix")));
    }

    @Test
    void noFilterRegexPattern() {
        assertTrue(RegexUtils.filterByPattern("topic1", List.of("^.*$")));
    }

    @Test
    void prefixWithRegexPattern() {
        List<String> pattern = List.of("^prefix.*$");
        assertTrue(RegexUtils.filterByPattern("prefix.topic1", pattern));
        assertTrue(RegexUtils.filterByPattern("prefix1.topic1", pattern));
        assertFalse(RegexUtils.filterByPattern("topic1", pattern));
    }

    @Test
    void prefixAndSuffixWithRegexPattern() {
        List<String> pattern = List.of("^abc.\\..*-test.$");
        assertTrue(RegexUtils.filterByPattern("abc1.topic-test2", pattern));
        assertTrue(RegexUtils.filterByPattern("abc1.stream-test2", pattern));
        assertFalse(RegexUtils.filterByPattern("abc1.topic-test20", pattern));
        assertFalse(RegexUtils.filterByPattern("abc.topic-test2", pattern));
        assertFalse(RegexUtils.filterByPattern("abc1.topic-test", pattern));
        assertFalse(RegexUtils.filterByPattern("abc1.topic-prod2", pattern));
    }

    @Test
    void filterWithMultipleRegexPattern() {
        List<String> pattern = List.of("^prefix1.*$", "^prefix2.*$");
        assertTrue(RegexUtils.filterByPattern("prefix1.topic", pattern));
        assertTrue(RegexUtils.filterByPattern("prefix2.topic", pattern));
        assertFalse(RegexUtils.filterByPattern("prefix3.topic", pattern));
        assertFalse(RegexUtils.filterByPattern("topic", pattern));
    }

    /**
     * Functional tests of the wildcard filter.
     */
    @Test
    void noFilterWildcard() {
        List<String> pattern1 = RegexUtils.wildcardStringsToRegexPatterns(List.of("*"));
        assertTrue(RegexUtils.filterByPattern("prefix.myTopic", pattern1));
        assertTrue(RegexUtils.filterByPattern("prefix10.yourSchema", pattern1));
        assertTrue(RegexUtils.filterByPattern("whatever.whatsoever", pattern1));
        assertTrue(RegexUtils.filterByPattern("whatever", pattern1));

        List<String> patterns2 = RegexUtils.wildcardStringsToRegexPatterns(List.of(""));
        assertTrue(RegexUtils.filterByPattern("prefix.myTopic", patterns2));
        assertTrue(RegexUtils.filterByPattern("prefix10.yourSchema", patterns2));
        assertTrue(RegexUtils.filterByPattern("whatever.whatsoever", patterns2));
        assertTrue(RegexUtils.filterByPattern("whatever", patterns2));
    }

    @Test
    void filterWithSimpleSuffixWildcard() {
        List<String> pattern = RegexUtils.wildcardStringsToRegexPatterns(List.of("abc.my*"));
        assertTrue(RegexUtils.filterByPattern("abc.myTopic", pattern));
        assertTrue(RegexUtils.filterByPattern("abc.myStream", pattern));
        assertTrue(RegexUtils.filterByPattern("abc.myConnect.xyz", pattern));
        assertTrue(RegexUtils.filterByPattern("abc.my", pattern));
        assertFalse(RegexUtils.filterByPattern("abc.topic", pattern));
        assertFalse(RegexUtils.filterByPattern("myTopic", pattern));
    }

    @Test
    void filterWithMultipleWildcard() {
        List<String> pattern = RegexUtils.wildcardStringsToRegexPatterns(List.of("abc.myT*op?c"));
        assertTrue(RegexUtils.filterByPattern("abc.myTopic", pattern));
        assertTrue(RegexUtils.filterByPattern("abc.myTopicTopic", pattern));
        assertTrue(RegexUtils.filterByPattern("abc.myTaaaaaopac", pattern));
        assertFalse(RegexUtils.filterByPattern("abc.myTopiiic", pattern));
        assertFalse(RegexUtils.filterByPattern("abc.yourTopic", pattern));
        assertFalse(RegexUtils.filterByPattern("abc.myTopic.suffix", pattern));
    }

    @Test
    void filterWithSuffixWildcard() {
        List<String> pattern = RegexUtils.wildcardStringsToRegexPatterns(List.of("*.myTopic?"));
        assertTrue(RegexUtils.filterByPattern("abc.myTopic1", pattern));
        assertTrue(RegexUtils.filterByPattern("prefix.myTopic2", pattern));
        assertFalse(RegexUtils.filterByPattern("abc.myTopic", pattern));
        assertFalse(RegexUtils.filterByPattern("abc.myTopic.suffix", pattern));
        assertFalse(RegexUtils.filterByPattern("abc.myTopic13", pattern));
    }

    @Test
    void filterWithMultipleSameWildcard() {
        List<String> pattern = RegexUtils.wildcardStringsToRegexPatterns(List.of("***.myTopic"));
        assertTrue(RegexUtils.filterByPattern("abc.myTopic", pattern));
        assertTrue(RegexUtils.filterByPattern("prefix.myTopic", pattern));
        assertTrue(RegexUtils.filterByPattern(".myTopic", pattern));
        assertFalse(RegexUtils.filterByPattern("prefix.myStream", pattern));
    }
}
