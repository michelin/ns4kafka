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
package com.michelin.ns4kafka.model;

import static com.michelin.ns4kafka.util.enumation.Kind.TABLEFLOW_TOPIC;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/** Tableflow configuration for an existing Kafka topic identified by metadata.name. */
@Data
@Serdeable
@EqualsAndHashCode(callSuper = true)
public class TableflowTopic extends Resource {
    @Valid @NotNull private TableflowTopicSpec spec;

    @Schema(description = "Server-side", accessMode = Schema.AccessMode.READ_ONLY)
    @EqualsAndHashCode.Exclude
    private TableflowTopicStatus status;

    /**
     * Constructor.
     *
     * @param metadata The resource metadata; name references the existing Kafka topic
     * @param spec The Tableflow configuration
     * @param status The Confluent creation status
     */
    @Builder
    public TableflowTopic(Metadata metadata, TableflowTopicSpec spec, TableflowTopicStatus status) {
        super("v1", TABLEFLOW_TOPIC, metadata);
        this.spec = spec;
        this.status = status;
    }

    /** Tableflow settings, managed independently from the Kafka topic settings. */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TableflowTopicSpec {
        @JsonProperty("table_formats")
        @NotEmpty @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Table formats allowed by the namespace.")
        private List<@NotBlank String> tableFormats;

        @Valid @NotNull @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Storage allowed by the namespace.")
        private Storage storage;
    }

    /** Table storage selection. */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Storage {
        @NotBlank private String kind;
    }

    /** Confluent identifier and materialization status updated by the executor. */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TableflowTopicStatus {
        private String id;
        private JsonNode state;
    }
}
