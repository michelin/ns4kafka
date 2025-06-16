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
package com.michelin.ns4kafka.util.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Topic configuration. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TopicConfig {
    public static final String PARTITIONS = "partitions";
    public static final String REPLICATION_FACTOR = "replication.factor";
    public static final String VALUE_SUBJECT_NAME_STRATEGY = "confluent.value.subject.name.strategy";

    public static final String TOPIC_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.TopicNameStrategy";
    public static final String TOPIC_RECORD_NAME_STRATEGY =
            "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy";
    public static final String RECORD_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.RecordNameStrategy";
}
