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

import static com.michelin.ns4kafka.util.enumation.Kind.CONSUMER_GROUP_RESET_OFFSET;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** Consumer group reset offsets. */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class ConsumerGroupResetOffsets extends MetadataResource {
    @Valid @NotNull private ConsumerGroupResetOffsetsSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec The spec
     */
    @Builder
    public ConsumerGroupResetOffsets(Metadata metadata, ConsumerGroupResetOffsetsSpec spec) {
        super("v1", CONSUMER_GROUP_RESET_OFFSET, metadata);
        this.spec = spec;
    }

    /** Represents the reset offsets method. */
    public enum ResetOffsetsMethod {
        TO_EARLIEST,
        TO_LATEST,
        TO_DATETIME, // string:yyyy-MM-ddTHH:mm:SS.sss
        BY_DURATION,
        SHIFT_BY,
        TO_OFFSET
    }

    /** Consumer group reset offsets specification. */
    @Getter
    @Setter
    @Builder
    @ToString
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConsumerGroupResetOffsetsSpec {
        @NotBlank private String topic;

        @NotNull private ResetOffsetsMethod method;

        private String options;
    }
}
