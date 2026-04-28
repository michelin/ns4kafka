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

import org.junit.jupiter.api.Test;

class TopicConfigUtilsTest {
    @Test
    void shouldTreatCleanupPolicyFormatsAsEquivalent() {
        assertTrue(TopicConfigUtils.areEquivalent("cleanup.policy", "delete, compact", "compact,delete"));
        assertTrue(TopicConfigUtils.areEquivalent("cleanup.policy", "delete,compact", "compact, delete"));
    }

    @Test
    void shouldDetectDifferentCleanupPolicies() {
        assertFalse(TopicConfigUtils.areEquivalent("cleanup.policy", "compact", "delete"));
    }

    @Test
    void shouldNotNormalizeSingleCleanupPolicyValues() {
        assertEquals("delete", TopicConfigUtils.normalizeValue("cleanup.policy", "delete"));
        assertEquals("compact", TopicConfigUtils.normalizeValue("cleanup.policy", "compact"));
    }

    @Test
    void shouldNotNormalizeOtherConfigs() {
        assertTrue(TopicConfigUtils.areEquivalent("retention.ms", "60000", "60000"));
        assertFalse(TopicConfigUtils.areEquivalent("retention.ms", "60000", "70000"));
    }

    @Test
    void shouldIdentifyDeleteAndCompactCleanupPolicy() {
        assertTrue(TopicConfigUtils.hasDeleteAndCompactCleanupPolicy("delete, compact"));
        assertTrue(TopicConfigUtils.hasDeleteAndCompactCleanupPolicy("compact,delete"));
        assertFalse(TopicConfigUtils.hasDeleteAndCompactCleanupPolicy("delete"));
        assertFalse(TopicConfigUtils.hasDeleteAndCompactCleanupPolicy("compact"));
    }
}
