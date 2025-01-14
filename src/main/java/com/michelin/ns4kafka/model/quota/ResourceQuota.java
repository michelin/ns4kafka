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

package com.michelin.ns4kafka.model.quota;

import static com.michelin.ns4kafka.util.enumation.Kind.RESOURCE_QUOTA;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Resource quota.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class ResourceQuota extends MetadataResource {
    @NotNull
    private Map<String, String> spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public ResourceQuota(Metadata metadata, Map<String, String> spec) {
        super("v1", RESOURCE_QUOTA, metadata);
        this.spec = spec;
    }

    /**
     * Resource quota spec keys.
     */
    @Getter
    @AllArgsConstructor
    public enum ResourceQuotaSpecKey {
        COUNT_TOPICS("count/topics"),
        COUNT_PARTITIONS("count/partitions"),
        DISK_TOPICS("disk/topics"),
        COUNT_CONNECTORS("count/connectors"),
        USER_PRODUCER_BYTE_RATE("user/producer_byte_rate"),
        USER_CONSUMER_BYTE_RATE("user/consumer_byte_rate");

        private final String key;

        @Override
        public String toString() {
            return key;
        }
    }
}
