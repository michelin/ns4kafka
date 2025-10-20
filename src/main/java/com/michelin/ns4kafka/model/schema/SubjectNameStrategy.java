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
package com.michelin.ns4kafka.model.schema;

import java.util.Arrays;
import lombok.AllArgsConstructor;

/** Schema subject naming strategies supported by Schema Registry. */
@AllArgsConstructor
public enum SubjectNameStrategy {
    TOPIC_NAME("io.confluent.kafka.serializers.subject.TopicNameStrategy"),
    TOPIC_RECORD_NAME("io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"),
    RECORD_NAME("io.confluent.kafka.serializers.subject.RecordNameStrategy");

    private static final String STRATEGY_PREFIX = "io.confluent.kafka.serializers.subject.";
    private final String value;

    /**
     * Convert the SubjectNameStrategy to its string representation.
     *
     * @return The string representation of the SubjectNameStrategy
     */
    @Override
    public String toString() {
        return value;
    }

    /**
     * Convenience method to get the expected format of the subject name according to the strategy.
     *
     * @return the expected format of the subject name
     */
    public String toExpectedFormat() {
        return switch (this) {
            case TOPIC_NAME -> "{topic}-{key|value}";
            case TOPIC_RECORD_NAME -> "{topic}-{recordName}";
            case RECORD_NAME -> "{recordName}";
        };
    }

    /**
     * Get the default SubjectNameStrategy.
     *
     * @return The default SubjectNameStrategy
     */
    public static SubjectNameStrategy defaultStrategy() {
        return TOPIC_NAME;
    }

    /**
     * Get SubjectNameStrategy from its string representation.
     *
     * @param stringValue The string representation of the SubjectNameStrategy
     * @return The SubjectNameStrategy
     */
    public static SubjectNameStrategy from(final String stringValue) {
        return Arrays.stream(values())
                .filter(s -> s.value.equals(stringValue))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown strategy: " + stringValue));
    }

    public String toShortName() {
        return value.replace(STRATEGY_PREFIX, "");
    }
}
