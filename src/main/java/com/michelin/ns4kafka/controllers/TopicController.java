package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ResourceSecurityPolicy;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.models.TopicSecurityPolicy;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.http.annotation.*;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller("/api/namespaces/{namespace}/topics")
public class TopicController {
    @Inject
    NamespaceRepository namespaceRepository;

    @Inject
    TopicRepository topicRepository;

    @Get("{topic}/produce")
    public Namespace produce(String namespace, String topic){
        Namespace n1 = new Namespace();
        n1.setName(namespace);
        n1.setDiskQuota(5);
        n1.setPolicies(List.of(new TopicSecurityPolicy("f4m.",
                ResourceSecurityPolicy.ResourcePatternType.PREFIXED,
                ResourceSecurityPolicy.SecurityPolicy.OWNER))
        );

        return namespaceRepository.createNamespace(n1);
    }

    /**
     * @param namespace The person's name
     * @param limit Optional restricts the scope of the return list
     * @return The list of all Topics available for that namespace (owned and accessible)
     */
    @Get("all")
    public List<String> list(String namespace, @Nullable @QueryValue TopicListLimit limit){
        return topicRepository.findAllForNamespace(namespace)
                .stream()
                .map(topic -> topic.getName())
                .collect(Collectors.toList());
    }

    @Post("{topic}")
    public Topic create(String namespace, @Body Topic topic){
        return topicRepository.create(topic);
    }

    public enum TopicListLimit {
        ALL,
        OWNED,
        ACCESS_GIVEN
    }
}
