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

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;

/** Schema subject naming strategies supported by Schema Registry. */
@AllArgsConstructor
public enum SubjectNameStrategy {
    TOPIC_NAME("TopicNameStrategy"),
    TOPIC_RECORD_NAME("TopicRecordNameStrategy"),
    RECORD_NAME("RecordNameStrategy");

    private final String value;

    /**
     * Convert the SubjectNameStrategy to its string representation.
     *
     * @return The string representation of the SubjectNameStrategy
     */
    @JsonValue
    @Override
    public String toString() {
        return value;
    }

    /**
     * Get the default SubjectNameStrategy.
     *
     * @return The default SubjectNameStrategy
     */
    public static List<SubjectNameStrategy> defaultStrategy() {
        return List.of(TOPIC_NAME);
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

    /**
     * Get the expected format for the subject name based on the strategy.
     *
     * @return The expected format for the subject name
     */
    public String toExpectedFormat() {
        return switch (this) {
            case TOPIC_NAME -> "{topic}-{key|value}";
            case TOPIC_RECORD_NAME -> "{topic}-{recordName}";
            case RECORD_NAME -> "{recordName}";
        };
    }
}
