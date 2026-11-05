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
package com.michelin.ns4kafka.service.client.confluent.entities;

import io.micronaut.serde.annotation.Serdeable;
import org.jspecify.annotations.Nullable;

/** Ownership and resource references returned by Confluent for a single API key. */
@Serdeable
public record ApiKeyMetadata(String id, @Nullable Spec spec) {
    @Serdeable
    public record Spec(@Nullable Reference owner, @Nullable Reference resource) {}

    @Serdeable
    public record Reference(String id) {}

    /**
     * Check the identity and scope of the returned key.
     *
     * @param apiKey The requested key ID
     * @param owner The expected service account ID
     * @param cluster The expected Kafka cluster ID
     * @return Whether the key matches all three identifiers
     */
    public boolean belongsTo(String apiKey, String owner, String cluster) {
        return apiKey.equals(id)
                && spec != null
                && spec.owner() != null
                && owner.equals(spec.owner().id())
                && spec.resource() != null
                && cluster.equals(spec.resource().id());
    }
}
