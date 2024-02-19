package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.michelin.ns4kafka.integration.TopicTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.models.AccessControlEntry.AclType;
import com.michelin.ns4kafka.models.AccessControlEntry.Permission;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourcePatternType;
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
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for ExceptionHandler.
 */
@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class ExceptionHandlerTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    HttpClient client;

    private String token;

    @BeforeAll
    void init() {
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

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<BearerAccessRefreshToken> response =
            client.toBlocking().exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1));
        client.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1));

        AccessControlEntry ns1acl = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-acl")
                .namespace("ns1")
                .build())
            .spec(AccessControlEntrySpec.builder()
                .resourceType(AclType.TOPIC)
                .resource("ns1-")
                .resourcePatternType(ResourcePatternType.PREFIXED)
                .permission(Permission.OWNER)
                .grantedTo("ns1")
                .build())
            .build();

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls").bearerAuth(token).body(ns1acl));
    }

    @Test
    void invalidTopicName() {
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
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(topicFirstCreate)));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        assertEquals("Invalid Resource", exception.getMessage());
        assertEquals("topic.metadata.name: must match \"^[a-zA-Z0-9_.-]+$\"",
            exception.getResponse().getBody(Status.class).get().getDetails().getCauses().get(0));
    }

    @Test
    void forbiddenTopic() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.GET, "/api/namespaces/ns2/topics")
                .bearerAuth(token)));

        assertEquals(HttpStatus.FORBIDDEN, exception.getStatus());
        assertEquals("Resource forbidden", exception.getMessage());
    }

    @Test
    void unauthorizedTopic() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.GET, "/api/namespaces/ns1/topics")));

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatus());
        assertEquals("Client '/': Unauthorized", exception.getMessage());
    }

    @Test
    void notFoundTopic() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> client.toBlocking()
                .exchange(HttpRequest.create(HttpMethod.GET, "/api/namespaces/ns1/topics/not-found-topic")
                    .bearerAuth(token)));

        assertEquals(HttpStatus.NOT_FOUND, exception.getStatus());
        assertEquals("Not Found", exception.getMessage());
    }

    @Test
    void notValidMethodTopic() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.PUT, "/api/namespaces/ns1/topics/")
                .bearerAuth(token)));

        assertEquals(HttpStatus.FORBIDDEN, exception.getStatus());
        assertEquals("Resource forbidden", exception.getMessage());
    }
}
