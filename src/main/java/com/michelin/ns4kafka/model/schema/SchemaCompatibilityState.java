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
package com.michelin.ns4kafka.model.schema;

import static com.michelin.ns4kafka.util.enumation.Kind.SCHEMA_COMPATIBILITY_STATE;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/** Schema compatibility state. */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class SchemaCompatibilityState extends MetadataResource {
    @Valid @NotNull private SchemaCompatibilityState.SchemaCompatibilityStateSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec The spec
     */
    @Builder
    public SchemaCompatibilityState(Metadata metadata, SchemaCompatibilityState.SchemaCompatibilityStateSpec spec) {
        super("v1", SCHEMA_COMPATIBILITY_STATE, metadata);
        this.spec = spec;
    }

    /** Schema compatibility state spec. */
    @Getter
    @Builder
    @ToString
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaCompatibilityStateSpec {
        @Builder.Default
        private final Schema.Compatibility compatibility = Schema.Compatibility.GLOBAL;
    }
}
