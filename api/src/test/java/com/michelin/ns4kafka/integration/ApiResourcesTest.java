package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.controllers.ApiResourcesController;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.List;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class ApiResourcesTest extends AbstractIntegrationTest {

    @Inject
    @Client("/")
    RxHttpClient client;

    @Test
    void asAdmin() {

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicTest.BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), TopicTest.BearerAccessRefreshToken.class).blockingFirst();

        String token = response.getBody().get().getAccessToken();
        List<ApiResourcesController.ResourceDefinition> resources = client.retrieve(
                HttpRequest.GET("/api-resources").bearerAuth(token),
                Argument.listOf(ApiResourcesController.ResourceDefinition.class)
        ).blockingFirst();

        Assertions.assertEquals(8, resources.size());

    }

    @Test
    void asAnonymous() {
        // This feature is not about restricting access, but easing user experience within the CLI
        // If the user is not authenticated, show everything
        List<ApiResourcesController.ResourceDefinition> resources = client.retrieve(
                HttpRequest.GET("/api-resources"),
                Argument.listOf(ApiResourcesController.ResourceDefinition.class)
        ).blockingFirst();

        Assertions.assertEquals(8, resources.size());
    }

    @Test
    void asUser() {
        Namespace ns1 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1")
                        .cluster("test-cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        RoleBinding rb1 = RoleBinding.builder()
                .metadata(ObjectMeta.builder()
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
        HttpResponse<TopicTest.BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), TopicTest.BearerAccessRefreshToken.class).blockingFirst();

        String token = response.getBody().get().getAccessToken();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1)).blockingFirst();

        UsernamePasswordCredentials userCredentials = new UsernamePasswordCredentials("user", "admin");
        HttpResponse<TopicTest.BearerAccessRefreshToken> userResponse = client.exchange(HttpRequest.POST("/login", userCredentials), TopicTest.BearerAccessRefreshToken.class).blockingFirst();
        String userToken = userResponse.getBody().get().getAccessToken();

        List<ApiResourcesController.ResourceDefinition> resources = client.retrieve(
                HttpRequest.GET("/api-resources").bearerAuth(userToken),
                Argument.listOf(ApiResourcesController.ResourceDefinition.class)
        ).blockingFirst();

        Assertions.assertEquals(2, resources.size());
    }
}
