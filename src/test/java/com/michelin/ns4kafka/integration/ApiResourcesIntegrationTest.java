package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.ns4kafka.controller.ApiResourcesController;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Api resources test.
 */
@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class ApiResourcesIntegrationTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void asAdmin() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response = client.toBlocking()
            .exchange(HttpRequest.POST("/login", credentials), TopicIntegrationTest.BearerAccessRefreshToken.class);

        String token = response.getBody().get().getAccessToken();
        List<ApiResourcesController.ResourceDefinition> resources = client.toBlocking().retrieve(
            HttpRequest.GET("/api-resources").bearerAuth(token),
            Argument.listOf(ApiResourcesController.ResourceDefinition.class));

        assertEquals(9, resources.size());
    }

    @Test
    void asAnonymous() {
        // This feature is not about restricting access, but easing user experience within the CLI
        // If the user is not authenticated, show everything
        List<ApiResourcesController.ResourceDefinition> resources = client.toBlocking().retrieve(
            HttpRequest.GET("/api-resources"),
            Argument.listOf(ApiResourcesController.ResourceDefinition.class));

        assertEquals(9, resources.size());
    }

    @Test
    void asUser() {
        Namespace ns1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns1")
                .cluster("test-cluster")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user1")
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        RoleBinding rb1 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns1-rb")
                .namespace("ns1")
                .build())
            .spec(RoleBinding.RoleBindingSpec.builder()
                .role(RoleBinding.Role.builder()
                    .resourceTypes(List.of("topics", "acls"))
                    .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                    .build())
                .subject(RoleBinding.Subject.builder()
                    .subjectName("userGroup")
                    .subjectType(RoleBinding.SubjectType.GROUP)
                    .build())
                .build())
            .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response = client.toBlocking()
            .exchange(HttpRequest.POST("/login", credentials), TopicIntegrationTest.BearerAccessRefreshToken.class);

        String token = response.getBody().get().getAccessToken();

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1));
        client.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1));

        UsernamePasswordCredentials userCredentials = new UsernamePasswordCredentials("user", "admin");
        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> userResponse = client.toBlocking()
            .exchange(HttpRequest.POST("/login", userCredentials), TopicIntegrationTest.BearerAccessRefreshToken.class);
        String userToken = userResponse.getBody().get().getAccessToken();

        List<ApiResourcesController.ResourceDefinition> resources = client.toBlocking().retrieve(
            HttpRequest.GET("/api-resources").bearerAuth(userToken),
            Argument.listOf(ApiResourcesController.ResourceDefinition.class));

        assertEquals(2, resources.size());
    }
}
