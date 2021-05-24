package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.KafkaConnectService;
import com.michelin.ns4kafka.services.SynchronizeNamespaceService;
import com.michelin.ns4kafka.services.TopicService;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.annotation.Status;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import java.util.ArrayList;
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
    KafkaConnectService kafkaConnectService;

    @Inject
    SynchronizeNamespaceService synchronizeNamespaceService;

    @Post
    public List<Object> synchronize(String namespace, @QueryValue(defaultValue = "false") boolean dryrun)
            throws ExecutionException, InterruptedException, TimeoutException {
        
        Namespace ns = getNamespace(namespace);

        if (ns == null)
            throw new ResourceNotFoundException();

        // Get ns4kfk access control entries for namespace 
        List<AccessControlEntry> accessControlEntries = accessControlEntryService.findAllGrantedToNamespace(ns);

        // Get list of ns4kfk topic names
        List<Topic> existingTopics = topicService.findAllForNamespace(ns);
        List<String> topicNames = existingTopics
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

        List<String> connectPatternToCreate = accessControlEntries
                .stream()
                .filter(accessControlEntry -> {
                            if (accessControlEntry.getSpec().getResourceType().equals(AccessControlEntry.ResourceType.CONNECT)
                                    && accessControlEntry.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)
                            ) {
                                return true;
                            }
                            return false;
                        }
                )
                .map(accessControlEntry -> accessControlEntry.getSpec().getResource())
                .collect(Collectors.toList());
        List<Connector> connectors =  synchronizeNamespaceService.buildConnectList(connectPatternToCreate);

        // Build Topic object for topics to create in ns4kfk
        List<Topic> topics = synchronizeNamespaceService.buildTopics(topicToCreate, namespace);

        // Create ns4kfk topics
        if (!dryrun) {
            HttpResponse.noContent();
        }
        topics.stream().forEach(topic -> topicService.create(topic));
        System.out.println(connectors);
        System.out.println(kafkaConnectService);
        System.out.println(ns);
        connectors.stream().forEach(connector -> kafkaConnectService.createOrUpdate(ns, connector));
        
        List<Object> list = new ArrayList<>();
        list.addAll(topics);
        list.addAll(connectors);
        return list;
    }

}
