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
package com.michelin.ns4kafka.model.connector;

import static com.michelin.ns4kafka.util.enumation.Kind.CONNECTOR;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
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

/** Connector. */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class Connector extends MetadataResource {
    @Valid @NotNull private ConnectorSpec spec;

    @EqualsAndHashCode.Exclude
    private ConnectorStatus status;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec The spec
     * @param status The status
     */
    @Builder
    public Connector(Metadata metadata, ConnectorSpec spec, ConnectorStatus status) {
        super("v1", CONNECTOR, metadata);
        this.spec = spec;
        this.status = status;
    }

    /** Connector task state. */
    public enum TaskState {
        // From
        // https://github.com/apache/kafka/blob/trunk/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/AbstractStatus.java
        UNASSIGNED,
        RUNNING,
        PAUSED,
        FAILED,
        DESTROYED,
    }

    /** Connector specification. */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectorSpec {
        @NotBlank private String connectCluster;

        @NotNull @JsonInclude(value = JsonInclude.Include.NON_ABSENT)
        private Map<String, String> config;
    }

    /** Connector status. */
    @Getter
    @Setter
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectorStatus {
        private TaskState state;
        private String workerId;
        private List<TaskStatus> tasks;

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;
    }

    /** Connector task status. */
    @Getter
    @Setter
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskStatus {
        String id;
        TaskState state;
        String trace;
        String workerId;
    }
}
