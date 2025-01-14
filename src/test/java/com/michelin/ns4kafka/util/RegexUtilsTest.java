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
    void shouldConvertDefaultStringToRegexPattern() {
        assertEquals(List.of("^.*$"), RegexUtils.convertWildcardStringsToRegex(List.of("")));
        assertEquals(List.of("^.*$"), RegexUtils.convertWildcardStringsToRegex(List.of("*")));
    }

    @Test
    void shouldConvertSimpleWildcardToRegexPattern() {
        assertEquals(List.of("^prefix.*$"), RegexUtils.convertWildcardStringsToRegex(List.of("prefix*")));
        assertEquals(List.of("^.*suffix$"), RegexUtils.convertWildcardStringsToRegex(List.of("*suffix")));
        assertEquals(List.of("^abc\\..*$"), RegexUtils.convertWildcardStringsToRegex(List.of("abc.*")));
        assertEquals(List.of("^item.$"), RegexUtils.convertWildcardStringsToRegex(List.of("item?")));
    }

    @Test
    void shouldConvertComplexWildcardToRegexPattern() {
        assertEquals(List.of("^prefix.*suffix$"), RegexUtils.convertWildcardStringsToRegex(List.of("prefix*suffix")));
        assertEquals(List.of("^...xyz$"), RegexUtils.convertWildcardStringsToRegex(List.of("???xyz")));
        assertEquals(List.of("^.*\\.topic.$"), RegexUtils.convertWildcardStringsToRegex(List.of("*.topic?")));
        assertEquals(List.of("^.*\\.topic.$"), RegexUtils.convertWildcardStringsToRegex(List.of("*.topic?")));
        assertEquals(List.of("^abc.\\..*-test.$"), RegexUtils.convertWildcardStringsToRegex(List.of("abc?.*-test?")));
    }

    @Test
    void shouldConvertMultipleWildcardsToRegexPatterns() {
        assertEquals(List.of("^prefix.*$", "^.*suffix$"),
            RegexUtils.convertWildcardStringsToRegex(List.of("prefix*", "*suffix")));
    }

    @Test
    void shouldResourceBeCoveredByWildcardRegexPattern() {
        assertTrue(RegexUtils.isResourceCoveredByRegex("topic1", List.of("^.*$")));
    }

    @Test
    void shouldResourceBeCoveredByPrefixRegexPattern() {
        List<String> pattern = List.of("^prefix.*$");
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix.topic", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix1.topic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.topic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("topic", pattern));
    }

    @Test
    void shouldResourceBeCoveredBySuffixRegexPattern() {
        List<String> pattern = List.of("^.*-dev$");
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.topic-dev", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("xyz.stream-dev", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.topic-dev2", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.topic-test", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.topic.dev", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("topic", pattern));
    }

    @Test
    void shouldResourceBeCoveredByComplexFilterRegexPattern() {
        List<String> pattern = List.of("^abc.\\..*-test.$");
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc1.topic-test2", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc1.stream-test2", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc1.topic-test20", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.topic-test2", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc1.topic-test", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc1.topic-prod2", pattern));
    }

    @Test
    void shouldResourceBeCoveredByAnyRegexPattern() {
        List<String> pattern = List.of("^prefix1.*$", "^prefix2.*$");
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix1.topic", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix2.topic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("prefix3.topic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("topic", pattern));
    }

    /**
     * Functional tests for wildcard filter.
     */
    @Test
    void shouldResourceBeCoveredByWildcardOnly() {
        List<String> pattern1 = RegexUtils.convertWildcardStringsToRegex(List.of("*"));
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix.myTopic", pattern1));
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix10.yourSchema", pattern1));
        assertTrue(RegexUtils.isResourceCoveredByRegex("whatever.whatsoever", pattern1));
        assertTrue(RegexUtils.isResourceCoveredByRegex("whatever", pattern1));

        List<String> patterns2 = RegexUtils.convertWildcardStringsToRegex(List.of(""));
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix.myTopic", patterns2));
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix10.yourSchema", patterns2));
        assertTrue(RegexUtils.isResourceCoveredByRegex("whatever.whatsoever", patterns2));
        assertTrue(RegexUtils.isResourceCoveredByRegex("whatever", patterns2));
    }

    @Test
    void shouldResourceBeCoveredByStringPrefixedWithWildcard() {
        List<String> pattern = RegexUtils.convertWildcardStringsToRegex(List.of("abc.my*"));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.myTopic", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.myStream", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.myConnect.xyz", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.my", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.topic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("myTopic", pattern));
    }

    @Test
    void shouldResourceBeCoveredByStringSuffixedWithWildcard() {
        List<String> pattern = RegexUtils.convertWildcardStringsToRegex(List.of("*-test"));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.myTopic-test", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("xyz.myStream-test", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("-test", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("myTopic-test", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.topic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("myTopic-dev", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("myTopic-test1", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("myTopic-test-dev", pattern));
    }

    @Test
    void shouldResourceBeCoveredByStringWithMultipleWildcards() {
        List<String> pattern = RegexUtils.convertWildcardStringsToRegex(List.of("abc.myT*op?c"));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.myTopic", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.myTopicTopic", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.myTaaaaaopac", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.myTopiiic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.yourTopic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.myTopic.suffix", pattern));
    }

    @Test
    void shouldResourceBeCoveredByStringWithPrefixAndSuffixWildcards() {
        List<String> pattern = RegexUtils.convertWildcardStringsToRegex(List.of("*.myTopic?"));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.myTopic1", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix.myTopic2", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.myTopic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.myTopic.suffix", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("abc.myTopic13", pattern));
    }

    @Test
    void shouldResourceBeCoveredByStringWithMultipleSameWildcards() {
        List<String> pattern = RegexUtils.convertWildcardStringsToRegex(List.of("***.myTopic"));
        assertTrue(RegexUtils.isResourceCoveredByRegex("abc.myTopic", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex("prefix.myTopic", pattern));
        assertTrue(RegexUtils.isResourceCoveredByRegex(".myTopic", pattern));
        assertFalse(RegexUtils.isResourceCoveredByRegex("prefix.myStream", pattern));
    }
}
