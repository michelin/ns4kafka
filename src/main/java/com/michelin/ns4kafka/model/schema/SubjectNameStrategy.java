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

/** Schema subject naming strategies supported by Schema Registry. */
public enum SubjectNameStrategy {
    TOPIC_NAME("io.confluent.kafka.serializers.subject.TopicNameStrategy"),
    TOPIC_RECORD_NAME("io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"),
    RECORD_NAME("io.confluent.kafka.serializers.subject.RecordNameStrategy");

    private final String STRATEGY_PREFIX = "io.confluent.kafka.serializers.subject";
    public static final SubjectNameStrategy DEFAULT = SubjectNameStrategy.TOPIC_NAME;

    private final String value;

    SubjectNameStrategy(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    /**
     * @return The format for subject value (i.e. the SchemaResource metadata name) according to subject name strategy
     */
    public String toExpectedFormat() {
        switch (this) {
            case TOPIC_NAME:
                return "{topic}-{key|value}";
            case TOPIC_RECORD_NAME:
                return "{topic}-{recordName}";
            case RECORD_NAME:
                return "{recordName}";
            default:
                throw new IllegalArgumentException("Unknown SubjectNameStrategy: " + this);
        }
    }

    /**
     * Return SubjectNameStrategy enum value corresponding to confluent's strategy value given as a String
     *
     * @param strategyRealValue
     * @return SubjectNameStrategy enum value from given confluent's strategy value
     */
    public static SubjectNameStrategy fromConfigValue(final String strategyRealValue) {
        return Arrays.stream(values())
                .filter(s -> s.value.equals(strategyRealValue))
                .findFirst()
                .orElseThrow();
    }

    public String toShortName() {
        return value.substring(STRATEGY_PREFIX.length() + 1);
    }
}
