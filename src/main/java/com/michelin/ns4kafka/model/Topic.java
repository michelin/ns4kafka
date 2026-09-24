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

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/** Topic. */
@Data
@Serdeable
@EqualsAndHashCode(callSuper = true)
public class Topic extends Resource {
    @Valid @NotNull private TopicSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec The spec
     */
    @Builder
    public Topic(Metadata metadata, TopicSpec spec) {
        super("v1", TOPIC, metadata);
        this.spec = spec;
    }

    /** Topic spec. */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopicSpec {
        private int replicationFactor;
        private int partitions;

        @Builder.Default
        private List<String> tags = new ArrayList<>();

        private String description;

        @Builder.Default
        private Map<String, String> configs = new HashMap<>();

        /**
         * Set the tags, defaulting to an empty list when null.
         *
         * @param tags The tags
         */
        public void setTags(List<String> tags) {
            this.tags = tags != null ? tags : new ArrayList<>();
        }

        /**
         * Set the configs, defaulting to an empty map when null.
         *
         * @param configs The configs
         */
        public void setConfigs(Map<String, String> configs) {
            this.configs = configs != null ? configs : new HashMap<>();
        }
    }
}
