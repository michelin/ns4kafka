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
package com.michelin.ns4kafka.service.client.confluent.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;
import java.util.List;

/** Request to enable Tableflow using the settings validated against the namespace. */
@Serdeable
public record TableflowTopicRequest(Spec spec) {
    /** Configuration scoped to the namespace's topic and managed cluster. */
    @Serdeable
    public record Spec(
            @JsonProperty("display_name") String displayName,
            Storage storage,
            @JsonProperty("table_formats") List<String> tableFormats,
            Reference environment,
            @JsonProperty("kafka_cluster") Reference kafkaCluster) {}

    /** Table storage configuration. */
    @Serdeable
    public record Storage(String kind) {}

    /** Confluent environment or cluster reference. */
    @Serdeable
    public record Reference(String id) {}
}
