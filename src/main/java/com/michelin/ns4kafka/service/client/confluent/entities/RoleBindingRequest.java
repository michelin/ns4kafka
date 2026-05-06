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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.property.ManagedClusterProperties.ConfluentCloudProperties;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record RoleBindingRequest(
        @NotNull String principal,
        @JsonProperty("role_name") @NotNull String roleName,
        @JsonProperty("crn_pattern") @NotNull String crnPattern) {

    public RoleBindingRequest(RoleBinding roleBinding, ConfluentCloudProperties properties) {
        this(
                roleBinding.principal(),
                roleBinding.roleName().toString(),
                "crn://confluent.cloud/organization=" + properties.getOrganizationId() + "/environment="
                        + properties.getEnvironmentId() + "/cloud-cluster=" + properties.getClusterId() + "/kafka="
                        + properties.getClusterId() + "/" + getResourceTypeString(roleBinding) + "="
                        + roleBinding.resource());
    }

    private static String getResourceTypeString(RoleBinding roleBinding) {
        return switch (roleBinding.resourceType()) {
            case TOPIC -> "topic";
            case TRANSACTIONAL_ID -> "transactional-id";
            case GROUP -> "group";
            default -> throw new IllegalArgumentException("Not implemented yet: " + roleBinding.resourceType());
        };
    }
}
