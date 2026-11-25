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

import com.fasterxml.jackson.annotation.JsonInclude;
import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/** Tableflow topic. */
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
     * @param metadata The metadata
     * @param spec The spec
     * @param status The status
     */
    @Builder
    public TableflowTopic(Metadata metadata, TableflowTopicSpec spec, TableflowTopicStatus status) {
        super("v1", TABLEFLOW_TOPIC, metadata);
        this.spec = spec;
        this.status = status;
    }

    /** Tableflow topic spec. */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TableflowTopicSpec {
        @NotEmpty @Schema(
                requiredMode = Schema.RequiredMode.REQUIRED,
                description = "Table formats to materialize the topic into.")
        private Set<@NotBlank String> tableFormats;

        @Valid @NotNull @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "Storage where the tables are written.")
        private Storage storage;

        @Min(value = 86_400_000, message = "must be greater than or equal to {value} ms (1 day)")
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Schema(description = "Maximum age of table snapshots or versions in milliseconds.")
        private Long snapshotRetentionMs;
    }

    /** Tableflow topic storage. */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Storage {
        @NotBlank private String kind;
    }

    /** Tableflow topic status. */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TableflowTopicStatus {
        private String id;
        private TableflowTopicState state;
    }

    /** Tableflow topic state. */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class TableflowTopicState {
        private String phase;
        private String errorMessage;
    }
}
