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

import static com.michelin.ns4kafka.security.ResourceBasedSecurityRule.RESOURCE_PATTERN;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;
import com.michelin.ns4kafka.util.enumation.Kind;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/** Resource. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Resource {
    private String apiVersion;
    private Kind kind;

    @Valid @NotNull private Metadata metadata;

    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Metadata {
        @NotBlank @Pattern(regexp = "^" + RESOURCE_PATTERN + "+$") private String name;

        private String namespace;
        private String cluster;
        private Map<String, String> labels;

        @EqualsAndHashCode.Exclude
        private int generation;

        @EqualsAndHashCode.Exclude
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date creationTimestamp;

        @EqualsAndHashCode.Exclude
        private Status status;

        @Data
        @Builder
        @Introspected
        @NoArgsConstructor
        @AllArgsConstructor
        public static class Status {
            private Phase phase;
            private String message;

            @JsonFormat(shape = JsonFormat.Shape.STRING)
            private Date lastUpdateTime;

            @JsonInclude(JsonInclude.Include.NON_NULL)
            private Map<String, String> options;

            public static Status ofSuccess() {
                return Status.builder()
                        .phase(Phase.SUCCESS)
                        .lastUpdateTime(Date.from(Instant.now()))
                        .build();
            }

            public static Status ofFailed(String message) {
                return Status.builder()
                        .phase(Phase.FAIL)
                        .message(message)
                        .lastUpdateTime(Date.from(Instant.now()))
                        .build();
            }

            public static Status ofPending() {
                return Status.builder()
                        .phase(Phase.PENDING)
                        .message("Awaiting processing by executor")
                        .build();
            }

            public static Status ofDeleting(Map<String, String> options) {
                return Status.builder()
                        .phase(Phase.DELETING)
                        .message("Awaiting deletion by executor")
                        .lastUpdateTime(Date.from(Instant.now()))
                        .options(options)
                        .build();
            }
        }

        public enum Phase {
            PENDING("Pending"),
            FAIL("Fail"),
            SUCCESS("Success"),
            DELETING("Deleting");

            private final String name;

            Phase(String name) {
                this.name = name;
            }

            @JsonValue
            @Override
            public String toString() {
                return name;
            }
        }
    }

    /**
     * Indicates whether the resource is pending deployment.
     *
     * @return {@code true} if it is, {@code false} otherwise
     */
    public boolean isPending() {
        if (metadata == null || metadata.getStatus() == null) {
            return false;
        }

        return metadata.getStatus().getPhase().equals(Metadata.Phase.PENDING);
    }

    /**
     * Indicates whether the resource is pending deletion.
     *
     * @return {@code true} if it is, {@code false} otherwise
     */
    public boolean isDeleting() {
        if (metadata == null || metadata.getStatus() == null) {
            return false;
        }

        return metadata.getStatus().getPhase().equals(Metadata.Phase.DELETING);
    }
}
