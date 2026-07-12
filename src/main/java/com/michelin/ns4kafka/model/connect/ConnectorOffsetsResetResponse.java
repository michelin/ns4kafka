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
package com.michelin.ns4kafka.model.connect;

import static com.michelin.ns4kafka.util.enumation.Kind.CONNECTOR_OFFSETS_RESET_RESPONSE;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.connect.ChangeConnectorState.ChangeConnectorStateStatus;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/** Connector offsets reset response. */
@Data
@Serdeable
@EqualsAndHashCode(callSuper = true)
public class ConnectorOffsetsResetResponse extends Resource {
    private ChangeConnectorStateStatus status;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param status The status
     */
    @Builder
    public ConnectorOffsetsResetResponse(Metadata metadata, ChangeConnectorStateStatus status) {
        super("v1", CONNECTOR_OFFSETS_RESET_RESPONSE, metadata);
        this.status = status;
    }
}
