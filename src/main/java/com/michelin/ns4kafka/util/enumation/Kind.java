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
package com.michelin.ns4kafka.util.enumation;

import com.fasterxml.jackson.annotation.JsonValue;

/** Resource kind. */
public enum Kind {
    ACCESS_CONTROL_ENTRY("AccessControlEntry"),
    CHANGE_CONNECTOR_STATE("ChangeConnectorState"),
    CONNECT_CLUSTER("ConnectCluster"),
    CONNECTOR("Connector"),
    CONSUMER_GROUP_RESET_OFFSET("ConsumerGroupResetOffsets"),
    CONSUMER_GROUP_RESET_OFFSET_RESPONSE("ConsumerGroupResetOffsetsResponse"),
    DELETE_RECORDS_RESPONSE("DeleteRecordsResponse"),
    KAFKA_USER_RESET_PASSWORD("KafkaUserResetPassword"),
    KAFKA_STREAM("KafkaStream"),
    NAMESPACE("Namespace"),
    RESOURCE_QUOTA("ResourceQuota"),
    RESOURCE_QUOTA_RESPONSE("ResourceQuotaResponse"),
    ROLE_BINDING("RoleBinding"),
    SCHEMA("Schema"),
    SCHEMA_COMPATIBILITY_STATE("SchemaCompatibilityState"),
    STATUS("Status"),
    TOPIC("Topic"),
    VAULT_RESPONSE("VaultResponse");

    @JsonValue
    private final String name;

    Kind(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
