package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Metadata;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.models.Status;
import com.michelin.ns4kafka.models.Topic;
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
 * Integration test for namespaces.
 */
@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class NamespaceTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    HttpClient client;

    private String token;

    @BeforeAll
    void init() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicTest.BearerAccessRefreshToken> response =
            client.toBlocking().exchange(HttpRequest.POST("/login", credentials),
                TopicTest.BearerAccessRefreshToken.class);

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

        assertEquals("Invalid Resource", exception.getMessage());
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
    void shouldInformWhenRequestedNamespaceIsUnknown() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("test-cluster")
                .labels(Map.of("support-group", "LDAP-GROUP-1"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user1")
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

        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,
                    "/api/namespaces/namespaceTypo/role-bindings")
                .bearerAuth(token).body(roleBinding)));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        assertEquals("Unknown namespace \"namespaceTypo\"", exception.getMessage());
    }
}
