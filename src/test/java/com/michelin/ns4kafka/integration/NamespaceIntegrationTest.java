package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.Status;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.BlockingHttpClient;
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
 * Integration test for namespaces.
 */
@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class NamespaceIntegrationTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    HttpClient client;

    private String token;

    @BeforeAll
    void init() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response =
            client.toBlocking().exchange(HttpRequest.POST("/login", credentials),
                TopicIntegrationTest.BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();
    }

    @Test
    void shouldValidateNamespaceNameWithAuthorizedChars() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("wrong*namespace")
                .cluster("test-cluster")
                .labels(Map.of("support-group", "LDAP-GROUP-1"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user1")
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> client.toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                    .bearerAuth(token)
                    .body(namespace)));

        assertEquals("Constraint validation failed", exception.getMessage());
        assertEquals("namespace.metadata.name: must match \"^[a-zA-Z0-9_.-]+$\"",
            exception.getResponse().getBody(Status.class).get().getDetails().getCauses().get(0));

        namespace.getMetadata().setName("accepted.namespace");

        var responseCreateNs = client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(namespace));

        assertEquals("created", responseCreateNs.header("X-Ns4kafka-Result"));

        var responseGetNs = client.toBlocking()
            .retrieve(HttpRequest.create(HttpMethod.GET, "/api/namespaces/accepted.namespace")
                .bearerAuth(token), Namespace.class);

        assertEquals(namespace.getSpec(), responseGetNs.getSpec());

        RoleBinding roleBinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("accepted.namespace-rb")
                .namespace("accepted.namespace")
                .build())
            .spec(RoleBinding.RoleBindingSpec.builder()
                .role(RoleBinding.Role.builder()
                    .resourceTypes(List.of("topics", "acls"))
                    .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                    .build())
                .subject(RoleBinding.Subject.builder()
                    .subjectName("group1")
                    .subjectType(RoleBinding.SubjectType.GROUP)
                    .build())
                .build())
            .build();

        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,
                "/api/namespaces/accepted.namespace/role-bindings")
            .bearerAuth(token)
            .body(roleBinding));

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("accepted.namespace-acl")
                .namespace("accepted.namespace")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resource("accepted.namespace.")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .grantedTo("accepted.namespace")
                .build())
            .build();

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/accepted.namespace/acls")
                .bearerAuth(token)
                .body(accessControlEntry));

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("accepted.namespace.topic")
                .namespace("accepted.namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,
                "/api/namespaces/accepted.namespace/topics")
            .bearerAuth(token).body(topic));

        var responseGetTopic = client.toBlocking().retrieve(
            HttpRequest.create(HttpMethod.GET, "/api/namespaces/accepted.namespace/topics/accepted.namespace.topic")
                .bearerAuth(token), Topic.class);

        assertEquals(topic.getSpec(), responseGetTopic.getSpec());
    }

    @Test
    void shouldFailWhenRequestedNamespaceIsUnknown() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("test-cluster")
                .labels(Map.of("support-group", "LDAP-GROUP-1"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user2")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        RoleBinding roleBinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns1-rb")
                .namespace("namespace")
                .build())
            .spec(RoleBinding.RoleBindingSpec.builder()
                .role(RoleBinding.Role.builder()
                    .resourceTypes(List.of("topics", "acls"))
                    .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                    .build())
                .subject(RoleBinding.Subject.builder()
                    .subjectName("group1")
                    .subjectType(RoleBinding.SubjectType.GROUP)
                    .build())
                .build())
            .build();

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token).body(namespace));

        var response = client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/namespace/role-bindings")
                .bearerAuth(token).body(roleBinding));

        assertEquals(HttpStatus.OK, response.getStatus());

        // Accessing unknown namespace
        HttpRequest<?> request = HttpRequest.create(HttpMethod.POST, "/api/namespaces/namespaceTypo/role-bindings")
            .bearerAuth(token).body(roleBinding);

        BlockingHttpClient blockingClient = client.toBlocking();

        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> blockingClient.exchange(request));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        assertEquals("Accessing unknown namespace \"namespaceTypo\"", exception.getMessage());
        assertTrue(exception.getResponse().getBody(Status.class).isPresent());
    }

    @Test
    void shouldFailWhenRequestedNamespaceIsForbidden() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace2")
                .cluster("test-cluster")
                .labels(Map.of("support-group", "LDAP-GROUP-1"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user3")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        RoleBinding roleBinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns1-rb")
                .namespace("namespace2")
                .build())
            .spec(RoleBinding.RoleBindingSpec.builder()
                .role(RoleBinding.Role.builder()
                    .resourceTypes(List.of("topics", "acls"))
                    .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                    .build())
                .subject(RoleBinding.Subject.builder()
                    .subjectName("group1")
                    .subjectType(RoleBinding.SubjectType.GROUP)
                    .build())
                .build())
            .build();

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token).body(namespace));

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/namespace2/role-bindings")
                .bearerAuth(token).body(roleBinding));

        Namespace otherNamespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace3")
                .cluster("test-cluster")
                .labels(Map.of("support-group", "LDAP-GROUP-1"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user4")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        RoleBinding otherRb = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns1-rb2")
                .namespace("namespace3")
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

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token).body(otherNamespace));

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/namespace3/role-bindings")
                .bearerAuth(token).body(otherRb));

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("user", "admin");

        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response =
            client.toBlocking().exchange(HttpRequest.POST("/login", credentials),
                TopicIntegrationTest.BearerAccessRefreshToken.class);

        String userToken = response.getBody().get().getAccessToken();

        // Accessing forbidden namespace when only having access to namespace3
        HttpRequest<?> request = HttpRequest.create(HttpMethod.GET, "/api/namespaces/namespace2/topics")
            .bearerAuth(userToken).body(roleBinding);

        BlockingHttpClient blockingClient = client.toBlocking();

        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> blockingClient.exchange(request));

        assertEquals(HttpStatus.FORBIDDEN, exception.getStatus());
        assertEquals("Accessing forbidden namespace \"namespace2\"", exception.getMessage());
        assertTrue(exception.getResponse().getBody(Status.class).isPresent());
    }
}
