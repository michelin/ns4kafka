package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ResourceSecurityPolicy;
import com.michelin.ns4kafka.models.Topic;

import java.util.List;

public class ResourceSecurityPolicyValidator {
    /***
     *
     * @param namespace
     * @param topic
     * @return true if the namespace is OWNER of this topic
     */
    public static boolean isNamespaceOwnerOnTopic(Namespace namespace, Topic topic) {
        return isNamespaceAllowedOnResource(namespace, topic.getMetadata().getName(), ResourceSecurityPolicy.ResourceType.TOPIC,
                List.of(ResourceSecurityPolicy.SecurityPolicy.OWNER));
    }
    public static boolean isNamespaceAccessOnTopic(Namespace namespace, Topic topic) {
        return isNamespaceAllowedOnResource(namespace, topic.getMetadata().getName(), ResourceSecurityPolicy.ResourceType.TOPIC,
                List.of(ResourceSecurityPolicy.SecurityPolicy.OWNER,
                        ResourceSecurityPolicy.SecurityPolicy.READ_WRITE,
                        ResourceSecurityPolicy.SecurityPolicy.READ));
    }
    public static boolean isNamespaceAllowedOnResource(Namespace namespace, String resource, ResourceSecurityPolicy.ResourceType resourceType, List<ResourceSecurityPolicy.SecurityPolicy> securityPolicies){
        return namespace.getPolicies()
                .stream()
                .anyMatch(resourceSecurityPolicy -> {
                    if(resourceSecurityPolicy.getResourceType() == resourceType &&  securityPolicies.contains(resourceSecurityPolicy.getSecurityPolicy())){
                        switch (resourceSecurityPolicy.getResourcePatternType()){
                            case REGEXP:
                                return resource.matches(resourceSecurityPolicy.getResource());
                            case PREFIXED:
                                return resource.startsWith(resourceSecurityPolicy.getResource());
                            case LITERAL:
                                return resource.equals(resourceSecurityPolicy.getResource());
                        }
                    }
                    return false;
                });
    }
}
