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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;
import java.util.Set;

/** Tableflow topic request. */
@Serdeable
public record TableflowTopicRequest(Spec spec) {
    /** Tableflow topic spec. */
    @Serdeable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Spec(
            @JsonProperty("display_name") String displayName,
            Storage storage,
            @JsonProperty("table_formats") Set<String> tableFormats,
            Reference environment,
            @JsonProperty("kafka_cluster") Reference kafkaCluster,
            Config config) {}

    /** Request to update a Tableflow topic in place. */
    @Serdeable
    public record Update(UpdateSpec spec) {}

    /** Writable Tableflow configuration and its cluster scope. */
    @Serdeable
    public record UpdateSpec(
            Reference environment,
            @JsonProperty("kafka_cluster") Reference kafkaCluster,
            Config config) {}

    /** Tableflow topic configuration. */
    @Serdeable
    public record Config(@JsonProperty("retention_ms") String retentionMs) {}

    /** Tableflow topic storage. */
    @Serdeable
    public record Storage(String kind) {}

    /** Confluent resource reference. */
    @Serdeable
    public record Reference(String id) {}
}
