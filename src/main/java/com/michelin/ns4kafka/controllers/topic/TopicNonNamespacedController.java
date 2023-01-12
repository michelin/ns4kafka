package com.michelin.ns4kafka.controllers.topic;

import com.michelin.ns4kafka.controllers.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.TopicService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;

import javax.annotation.security.RolesAllowed;
import java.util.List;

@Tag(name = "Topics")
@Controller(value = "/api/topics")
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class TopicNonNamespacedController extends NonNamespacedResourceController {
    @Inject
    TopicService topicService;

    /**
     * Get all the topics of all namespaces
     * @return A list of topics
     */
    @Get
    public List<Topic> listAll() {
        return topicService.findAll();
    }
}
