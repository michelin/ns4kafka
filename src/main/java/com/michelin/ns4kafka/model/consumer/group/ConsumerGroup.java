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
package com.michelin.ns4kafka.model.consumer.group;

import static com.michelin.ns4kafka.util.enumation.Kind.CONSUMER_GROUP;

import com.michelin.ns4kafka.model.Resource;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.GroupState;

/** Consumer group. */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class ConsumerGroup extends Resource {
    @EqualsAndHashCode.Exclude
    @Valid @NotNull private ConsumerGroupStatus status;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param status The status
     */
    @Builder
    public ConsumerGroup(Resource.Metadata metadata, ConsumerGroupStatus status) {
        super("v1", CONSUMER_GROUP, metadata);
        this.status = status;
    }

    /** Consumer group status. */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    @Schema(description = "Server-side", accessMode = Schema.AccessMode.READ_ONLY)
    public static class ConsumerGroupStatus {
        private GroupState state;

        @Builder.Default
        private List<ConsumerGroupOffset> offsets = new ArrayList<>();
    }

    /** Consumer group committed offset. */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConsumerGroupOffset {
        private String topic;
        private int partition;
        private long currentOffset;
    }
}
