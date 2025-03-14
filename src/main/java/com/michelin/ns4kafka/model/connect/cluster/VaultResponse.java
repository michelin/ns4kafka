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
package com.michelin.ns4kafka.model.connect.cluster;

import static com.michelin.ns4kafka.util.enumation.Kind.VAULT_RESPONSE;

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

/** Represents the Kafka Connect Cluster Vault Response. */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class VaultResponse extends MetadataResource {
    /** The vault resource spec. */
    @Valid @NotNull private VaultResponseSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec The spec
     */
    @Builder
    public VaultResponse(Metadata metadata, VaultResponseSpec spec) {
        super("v1", VAULT_RESPONSE, metadata);
        this.spec = spec;
    }

    /** Represents the vault response specification. */
    @Getter
    @Builder
    @ToString
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VaultResponseSpec {
        /** The clear text to encrypt. */
        private String clearText;

        /** The encrypted text. */
        private String encrypted;
    }
}
