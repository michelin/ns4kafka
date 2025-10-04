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

import static com.michelin.ns4kafka.util.enumation.Kind.ACCESS_CONTROL_ENTRY;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/** Access control entry. */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class AccessControlEntry extends MetadataResource {
    @Valid @NotNull private AccessControlEntrySpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec The spec
     */
    @Builder
    public AccessControlEntry(Metadata metadata, AccessControlEntrySpec spec) {
        super("v1", ACCESS_CONTROL_ENTRY, metadata);
        this.spec = spec;
    }

    /** Resource type managed by Ns4Kafka. */
    public enum ResourceType {
        TOPIC,
        GROUP,
        CONNECT,
        CONNECT_CLUSTER,
        SCHEMA
    }

    /** Resource pattern type. */
    public enum ResourcePatternType {
        LITERAL,
        PREFIXED
    }

    /** Permission. */
    public enum Permission {
        OWNER,
        READ,
        WRITE
    }

    /** Access control entry specification. */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AccessControlEntrySpec {
        @NotNull protected AccessControlEntry.ResourceType resourceType;

        @NotNull @NotBlank protected String resource;

        @NotNull protected ResourcePatternType resourcePatternType;

        @NotNull protected Permission permission;

        @NotBlank @NotNull protected String grantedTo;
    }
}
