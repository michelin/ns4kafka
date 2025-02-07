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

import static com.michelin.ns4kafka.util.enumation.Kind.TOPIC;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Topic.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class Topic extends MetadataResource {
    @Valid
    @NotNull
    private TopicSpec spec;

    @EqualsAndHashCode.Exclude
    private TopicStatus status;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     * @param status   The status
     */
    @Builder
    public Topic(Metadata metadata, TopicSpec spec, TopicStatus status) {
        super("v1", TOPIC, metadata);
        this.spec = spec;
        this.status = status;
    }

    /**
     * Topic spec.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopicSpec {
        private int replicationFactor;
        private int partitions;
        @Builder.Default
        @JsonSetter(nulls = Nulls.AS_EMPTY)
        private List<String> tags = new ArrayList<>();
        private String description;
        private Map<String, String> configs;
    }

    /**
     * Topic status.
     */
    @Getter
    @Setter
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Schema(description = "Server-side", accessMode = Schema.AccessMode.READ_ONLY)
    public static class TopicStatus {
        private TopicPhase phase;
        private String message;

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;

        /**
         * Success status.
         *
         * @param message A success message
         * @return A success topic status
         */
        public static TopicStatus ofSuccess(String message) {
            return TopicStatus.builder()
                .phase(TopicPhase.Success)
                .message(message)
                .lastUpdateTime(Date.from(Instant.now()))
                .build();
        }

        /**
         * Failed status.
         *
         * @param message A failure message
         * @return A failure topic status
         */
        public static TopicStatus ofFailed(String message) {
            return TopicStatus.builder()
                .phase(TopicPhase.Failed)
                .message(message)
                .lastUpdateTime(Date.from(Instant.now()))
                .build();
        }

        /**
         * Pending status.
         *
         * @return A pending topic status
         */
        public static TopicStatus ofPending() {
            return Topic.TopicStatus.builder()
                .phase(Topic.TopicPhase.Pending)
                .message("Awaiting processing by executor")
                .build();
        }
    }

    /**
     * Topic phase.
     */
    public enum TopicPhase {
        Pending,
        Success,
        Failed
    }
}
