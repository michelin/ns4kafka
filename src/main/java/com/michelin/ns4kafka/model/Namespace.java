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

import static com.michelin.ns4kafka.util.enumation.Kind.NAMESPACE;

import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Namespace.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class Namespace extends MetadataResource {
    @Valid
    @NotNull
    private NamespaceSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public Namespace(Metadata metadata, NamespaceSpec spec) {
        super("v1", NAMESPACE, metadata);
        this.spec = spec;
    }

    /**
     * Namespace spec.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NamespaceSpec {
        @NotBlank
        private String kafkaUser;
        private boolean protectionEnabled;
        @Builder.Default
        private List<String> connectClusters = List.of();
        private TopicValidator topicValidator;
        private ConnectValidator connectValidator;
    }
}