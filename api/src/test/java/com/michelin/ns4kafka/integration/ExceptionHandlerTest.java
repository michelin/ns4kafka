package com.michelin.ns4kafka.integration;

import javax.inject.Inject;

import com.michelin.ns4kafka.integration.TopicTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.models.AccessControlEntry.Permission;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.models.RoleBinding.Role;
import com.michelin.ns4kafka.models.RoleBinding.RoleBindingSpec;
import com.michelin.ns4kafka.models.RoleBinding.Subject;
import com.michelin.ns4kafka.models.RoleBinding.SubjectType;
import com.michelin.ns4kafka.models.RoleBinding.Verb;
import com.michelin.ns4kafka.models.Status;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.models.Topic.TopicSpec;
import com.michelin.ns4kafka.validation.TopicValidator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Disabled
@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class ExceptionHandlerTest extends AbstractIntegrationTest {

    @Inject
    @Client("/")
    RxHttpClient client;

    private String token;

    @BeforeAll
    void init(){
        Namespace ns1 = Namespace.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1")
                      .cluster("test-cluster")
                      .build())
            .spec(NamespaceSpec.builder()
                  .kafkaUser("user1")
                  .connectClusters(List.of("test-connect"))
                  .topicValidator(TopicValidator.makeDefaultOneBroker())
                  .build())
            .build();

        RoleBinding rb1 = RoleBinding.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-rb")
                      .namespace("ns1")
                      .build())
            .spec(RoleBindingSpec.builder()
                  .role(Role.builder()
                        .resourceTypes(List.of("topics", "acls"))
                        .verbs(List.of(Verb.POST, Verb.GET))
                        .build())
                  .subject(Subject.builder()
                           .subjectName("group1")
                           .subjectType(SubjectType.GROUP)
                           .build())
                  .build())
            .build();

        AccessControlEntry ns1acl = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-acl")
                      .namespace("ns1")
                      .build())
            .spec(AccessControlEntrySpec.builder()
                  .resourceType(ResourceType.TOPIC)
                  .resource("ns1-")
                  .resourcePatternType(ResourcePatternType.PREFIXED)
                  .permission(Permission.OWNER)
                  .grantedTo("ns1")
                  .build())
            .build();


        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class).blockingFirst();

        token = response.getBody().get().getAccessToken();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(ns1acl)).blockingFirst();
    }

    @Test
    void invalidTopicName() throws InterruptedException, ExecutionException {

        Topic topicFirstCreate = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-invalid-é")
                        .namespace("ns1")
                        .build())
                .spec(TopicSpec.builder()
                        .partitions(3)
                        .replicationFactor(1)
                        .configs(Map.of("cleanup.policy", "delete",
                                "min.insync.replicas", "1",
                                "retention.ms", "60000"))
                        .build())
                .build();
        HttpClientResponseException exception = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics")
                        .bearerAuth(token)
                        .body(topicFirstCreate))
                        .blockingFirst());

        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        Assertions.assertEquals("Invalid Resource", exception.getMessage());
        Assertions.assertEquals("topic.metadata.name: must match \"^[a-zA-Z0-9_.-]+$\"", exception.getResponse().getBody(Status.class).get().getDetails().getCauses().get(0));
    }

    @Test
    void forbiddenTopic() throws InterruptedException, ExecutionException {

        HttpClientResponseException exception = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns2/topics")
                        .bearerAuth(token))
                        .blockingFirst());

        Assertions.assertEquals(HttpStatus.FORBIDDEN, exception.getStatus());
        Assertions.assertEquals("Resource forbidden", exception.getMessage());
    }

    @Test
    void UnauthorizedTopic() throws InterruptedException, ExecutionException {

        HttpClientResponseException exception = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns1/topics"))
                        .blockingFirst());

        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatus());
        Assertions.assertEquals("Unauthorized", exception.getMessage());
    }

    @Test
    void notFoundTopic() throws InterruptedException, ExecutionException {

        HttpClientResponseException exception = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns1/topics/not-found-topic")
                        .bearerAuth(token))
                        .blockingFirst());

        Assertions.assertEquals(HttpStatus.NOT_FOUND, exception.getStatus());
        Assertions.assertEquals("Not Found", exception.getMessage());
    }

    @Test
    void notValidMethodTopic() throws InterruptedException, ExecutionException {

        HttpClientResponseException exception = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.PUT,"/api/namespaces/ns1/topics/")
                        .bearerAuth(token))
                        .blockingFirst());

        Assertions.assertEquals(HttpStatus.FORBIDDEN, exception.getStatus());
        Assertions.assertEquals("Resource forbidden", exception.getMessage());
    }
}
