package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.SynchronizeNamespaceService;
import com.michelin.ns4kafka.services.TopicService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Tag(name = "Synchronize")
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Controller(value = "/api/namespaces/{namespace}/synchronize/")
public class SynchronizeNamespaceController extends NamespacedResourceController {

    @Inject
    AccessControlEntryService accessControlEntryService;

    @Inject
    TopicService topicService;
    
    @Inject
    SynchronizeNamespaceService synchronizeNamespaceService;

    @Post
    public List<Topic> synchronize(String namespace, @QueryValue(defaultValue = "false") boolean dryrun) 
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = getNamespace(namespace);

        if (ns == null)
            throw new ResourceNotFoundException();

        // Get ns4kfk access control entries for namespace 
        List<AccessControlEntry> accessControlEntries = accessControlEntryService.findAllGrantedToNamespace(ns);

        // Get list of ns4kfk topic names
        List<Topic> topics = topicService.findAllForNamespace(ns);
        List<String> topicNames = topics
                .stream()
                .map(topic -> 
                        topic.getMetadata().getName()).collect(Collectors.toList()
                );

        // Filter the list to create between existing ns4kfk topics and cluster topics
        List<String> topicToCreate = accessControlEntries                
                .stream()
                .filter(accessControlEntry -> {
                            if (accessControlEntry.getSpec().getResourceType().equals(AccessControlEntry.ResourceType.TOPIC)
                                    && accessControlEntry.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.LITERAL)
                                    && !topicNames.contains(accessControlEntry.getSpec().getResource())) {
                                return true;
                            }
                            return false;
                        }
                )
                .map(accessControlEntry -> accessControlEntry.getSpec().getResource())
                .collect(Collectors.toList());

        // Build Topic object for topics to create in ns4kfk
        List<Topic> list = synchronizeNamespaceService.buildTopics(topicToCreate, namespace);
        
        // Create ns4kfk topics
        if(dryrun) {
            return list;
        }
        list.stream().forEach(topic -> topicService.create(topic));

        return list;
    }

}
