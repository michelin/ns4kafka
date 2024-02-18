package com.michelin.ns4kafka.models;

public abstract class Kind {
    public static final String ACL = "AccessControlEntry";
    public static final String CONNECTOR = "Connector";
    public static final String CONNECTOR_CLUSTER = "ConnectCluster";
    public static final String CONSUMER_GROUP = "ConsumerGroup";
    public static final String CONSUMER_GROUP_RESET = "ConsumerGroupResetOffsets";
    public static final String KAFKA_STREAM = "KafkaStream";
    public static final String KAFKA_USER = "KafkaUserResetPassword";
    public static final String NAMESPACE = "Namespace";
    public static final String RESOURCE_QUOTA = "ResourceQuota";
    public static final String ROLE_BINDING = "RoleBinding";
    public static final String SCHEMA = "Schema";
    public static final String SCHEMA_COMPAT = "SchemaCompatibilityState";
    public static final String STATUS = "Status";
    public static final String TOPIC = "Topic";
}
