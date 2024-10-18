package com.michelin.ns4kafka.util.enumation;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Resource kind.
 */
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
