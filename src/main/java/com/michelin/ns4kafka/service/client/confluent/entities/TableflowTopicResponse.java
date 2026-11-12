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
import com.michelin.ns4kafka.model.TableflowTopic;
import io.micronaut.serde.annotation.Serdeable;
import java.util.Set;
import lombok.Builder;

/** Tableflow topic response. */
@Builder
@Serdeable
public record TableflowTopicResponse(String id, Spec spec, Status status) {
    /**
     * Map the Confluent materialization status to its Ns4Kafka representation.
     *
     * @return The Tableflow topic status
     */
    public TableflowTopic.TableflowTopicStatus toTopicStatus() {
        return TableflowTopic.TableflowTopicStatus.builder()
                .id(id)
                .state(
                        status == null
                                ? null
                                : TableflowTopic.TableflowTopicState.builder()
                                        .phase(status.phase())
                                        .errorMessage(status.errorMessage())
                                        .build())
                .build();
    }

    /** Tableflow topic spec. */
    @Builder
    @Serdeable
    public record Spec(
            @JsonProperty("display_name") String displayName,
            @JsonProperty("table_formats") Set<String> tableFormats,
            Storage storage) {}

    /** Tableflow topic storage. */
    @Builder
    @Serdeable
    public record Storage(String kind) {}

    /** Tableflow topic status. */
    @Builder
    @Serdeable
    public record Status(
            String phase, @JsonProperty("error_message") String errorMessage) {}
}
