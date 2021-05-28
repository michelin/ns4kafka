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
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
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

        // Get the list of topics with literal accessControlEntry that exists in ns4kfk
        List<String> literalTopicToCreate = accessControlEntries
                .stream()
                .filter(accessControlEntry -> {
                            if (accessControlEntry.getSpec().getResourceType().equals(AccessControlEntry.ResourceType.TOPIC)
                                    && accessControlEntry.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.LITERAL)) {
                                return true;
                            }
                            return false;
                        }
                )
                .map(accessControlEntry -> accessControlEntry.getSpec().getResource())
                .collect(Collectors.toList());

        // Get the list of topics with prefixed accessControlEntry that exists in ns4kfk
        List<String> prefixedTopicToCreate = accessControlEntries
                .stream()
                .filter(accessControlEntry -> {
                            if (accessControlEntry.getSpec().getResourceType().equals(AccessControlEntry.ResourceType.TOPIC)
                                    && accessControlEntry.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)) {
                                return true;
                            }
                            return false;
                        }
                )
                .map(accessControlEntry -> accessControlEntry.getSpec().getResource())
                .collect(Collectors.toList());

        //  Get the list of connectors with prefixed accessControlEntry that exists in ns4kfk
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

        // Build Topic and Connect objects for topics to create in ns4kfk
        List<Topic> literalTopics = synchronizeNamespaceService.buildLiteralTopics(literalTopicToCreate, namespace);
        List<Topic> prefixedTopics = synchronizeNamespaceService.buildPrefixedTopics(prefixedTopicToCreate, namespace);
        List<Connector> connectors = synchronizeNamespaceService.buildConnectList(connectPatternToCreate);

        List<Object> list = new ArrayList<>();
        list.addAll(literalTopics);
        list.addAll(prefixedTopics);
        list.addAll(connectors);

        // Create ns4kfk topics
        
        // if dry run, do nothing
        if (dryrun) {
            return list;
        }

        literalTopics.stream().forEach(topic -> topicService.create(topic));
        prefixedTopics.stream().forEach(topic -> topicService.create(topic));
        connectors.stream().forEach(connector -> kafkaConnectService.createOrUpdate(ns, connector));
        
        return list;
    }

}
