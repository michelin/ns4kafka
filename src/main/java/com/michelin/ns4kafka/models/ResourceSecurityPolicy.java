package com.michelin.ns4kafka.models;

import lombok.Data;

@Data
public class ResourceSecurityPolicy {
    protected ResourceType resourceType;
    protected String resource;
    protected ResourcePatternType resourcePatternType;
    protected SecurityPolicy securityPolicy;

    public enum ResourceType {
        TOPIC,
        CONSUMER_GROUP,
        CONNECT,
        SCHEMA
    }
    public enum ResourcePatternType {
        LITERAL,
        PREFIXED,
        REGEXP
    }
    public enum SecurityPolicy {
        OWNER,
        READ,
        READ_WRITE
    }
}
