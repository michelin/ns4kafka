package com.michelin.ns4kafka.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ResourceSecurityPolicy {
    protected ResourceType resourceType;
    protected String resource;
    protected ResourcePatternType resourcePatternType;
    protected SecurityPolicy securityPolicy;

    public enum ResourceType {
        TOPIC,
        GROUP,
        CONNECT,
        SCHEMA
    }
    public enum ResourcePatternType {
        LITERAL,
        PREFIXED
        //TODO handle REGEX properly. Kafka ACL doesn't have Regex, only LITERAL and PREFIXED
        //REGEXP
    }
    public enum SecurityPolicy {
        OWNER,
        READ,
        WRITE
    }
    public static List<ResourceSecurityPolicy> buildDefaultNamespacePolicies(String prefix){
        List<ResourceSecurityPolicy> list = new ArrayList<>();

        ResourceSecurityPolicy rsp = new ResourceSecurityPolicy();
        rsp.setResource(prefix);
        rsp.setSecurityPolicy(SecurityPolicy.OWNER);
        rsp.setResourceType(ResourceType.TOPIC);
        rsp.setResourcePatternType(ResourcePatternType.PREFIXED);
        list.add(rsp);

        /*rsp = new ResourceSecurityPolicy();
        rsp.setResource(prefix);
        rsp.setSecurityPolicy(SecurityPolicy.OWNER);
        rsp.setResourceType(ResourceType.CONNECT);
        rsp.setResourcePatternType(ResourcePatternType.PREFIXED);
        list.add(rsp);*/

        rsp = new ResourceSecurityPolicy();
        rsp.setResource(prefix);
        rsp.setSecurityPolicy(SecurityPolicy.OWNER);
        rsp.setResourceType(ResourceType.GROUP);
        rsp.setResourcePatternType(ResourcePatternType.PREFIXED);
        list.add(rsp);

        rsp = new ResourceSecurityPolicy();
        rsp.setResource("connect-"+prefix);
        rsp.setSecurityPolicy(SecurityPolicy.OWNER);
        rsp.setResourceType(ResourceType.GROUP);
        rsp.setResourcePatternType(ResourcePatternType.PREFIXED);
        list.add(rsp);

        /*rsp = new ResourceSecurityPolicy();
        rsp.setResource(prefix);
        rsp.setSecurityPolicy(SecurityPolicy.OWNER);
        rsp.setResourceType(ResourceType.SCHEMA);
        rsp.setResourcePatternType(ResourcePatternType.PREFIXED);
        list.add(rsp);*/

        return list;
    }
}
