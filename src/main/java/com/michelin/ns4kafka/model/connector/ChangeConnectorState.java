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

import static com.michelin.ns4kafka.util.enumation.Kind.CHANGE_CONNECTOR_STATE;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Change connector state.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class ChangeConnectorState extends MetadataResource {
    @Valid
    @NotNull
    private ChangeConnectorStateSpec spec;
    private ChangeConnectorStateStatus status;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     * @param status   The status
     */
    @Builder
    public ChangeConnectorState(Metadata metadata, ChangeConnectorStateSpec spec,
                                ChangeConnectorStateStatus status) {
        super("v1", CHANGE_CONNECTOR_STATE, metadata);
        this.spec = spec;
        this.status = status;
    }

    /**
     * Connector action.
     */
    public enum ConnectorAction {
        PAUSE,
        RESUME,
        RESTART;

        /**
         * Build connector action from string.
         *
         * @param key the key
         * @return the connector action
         */
        @JsonCreator
        public static ConnectorAction fromString(String key) {
            for (ConnectorAction type : ConnectorAction.values()) {
                if (type.name().equalsIgnoreCase(key)) {
                    return type;
                }
            }
            return null;
        }
    }

    /**
     * Change connector state specification.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChangeConnectorStateSpec {
        @NotNull
        private ConnectorAction action;
    }

    /**
     * Change connector state status.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChangeConnectorStateStatus {
        private boolean success;
        private HttpStatus code;
        private String errorMessage;
    }
}
