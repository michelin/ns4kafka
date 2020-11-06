package com.michelin.ns4kafka.models.security;

public class TopicSecurityPolicy extends ResourceSecurityPolicy{

    public TopicSecurityPolicy(String resource, ResourcePatternType resourcePatternType, SecurityPolicy securityPolicy){
        this.resourceType = ResourceType.TOPIC;
        this.resource = resource;
        this.resourcePatternType = resourcePatternType;
        this.securityPolicy = securityPolicy;
    }
}
