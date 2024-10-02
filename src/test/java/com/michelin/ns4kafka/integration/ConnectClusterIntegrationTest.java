package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.integration.TopicIntegrationTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.integration.container.KafkaConnectIntegrationTest;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.model.AccessControlEntry.Permission;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.RoleBinding.Role;
import com.michelin.ns4kafka.model.RoleBinding.RoleBindingSpec;
import com.michelin.ns4kafka.model.RoleBinding.Subject;
import com.michelin.ns4kafka.model.RoleBinding.SubjectType;
import com.michelin.ns4kafka.model.RoleBinding.Verb;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.model.connect.cluster.VaultResponse;
import com.michelin.ns4kafka.service.executor.ConnectorAsyncExecutor;
import com.michelin.ns4kafka.service.executor.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.ApplicationContext;
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
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@MicronautTest
class ConnectClusterIntegrationTest extends KafkaConnectIntegrationTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    @Client("/")
    private HttpClient ns4KafkaClient;

    private HttpClient connectClient;

    @Inject
    private List<TopicAsyncExecutor> topicAsyncExecutorList;

    @Inject
    private List<ConnectorAsyncExecutor> connectorAsyncExecutorList;

    private String token;

    @BeforeAll
    void init() {
        // Create HTTP client as bean to load client configuration from application.yml
        connectClient = applicationContext.createBean(HttpClient.class, getConnectUrl());

        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns1")
                .cluster("test-cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .connectValidator(ConnectValidator.builder()
                    .validationConstraints(Map.of())
                    .sinkValidationConstraints(Map.of())
                    .classValidationConstraints(Map.of())
                    .build())
                .build())
            .build();

        RoleBinding roleBinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns1-rb")
                .namespace("ns1")
                .build())
            .spec(RoleBindingSpec.builder()
                .role(Role.builder()
                    .resourceTypes(List.of(
                            "topics",
                            "acls",
                            "connect-clusters",
                            "connect-clusters/vaults",
                            "connectors"))
                    .verbs(List.of(Verb.POST, Verb.GET, Verb.PUT, Verb.DELETE))
                    .build())
                .subject(Subject.builder()
                    .subjectName("group1")
                    .subjectType(SubjectType.GROUP)
                    .build())
                .build())
            .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<BearerAccessRefreshToken> response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(namespace));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings")
                .bearerAuth(token)
                .body(roleBinding));

        AccessControlEntry aclConnect = AccessControlEntry.builder()
                .metadata(Metadata.builder()
                        .name("ns1-acl")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntrySpec.builder()
                        .resourceType(ResourceType.CONNECT)
                        .resource("ns1-")
                        .resourcePatternType(ResourcePatternType.PREFIXED)
                        .permission(Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                        .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                        .bearerAuth(token)
                        .body(aclConnect));

        AccessControlEntry aclConnectCluster = AccessControlEntry.builder()
                .metadata(Metadata.builder()
                        .name("ns1-acl-cc")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntrySpec.builder()
                        .resourceType(ResourceType.CONNECT_CLUSTER)
                        .resource("ns1-")
                        .resourcePatternType(ResourcePatternType.PREFIXED)
                        .permission(Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                        .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                        .bearerAuth(token)
                        .body(aclConnectCluster));

        AccessControlEntry aclTopic = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-acl-topic")
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

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(aclTopic));
    }

    @Test
    void shouldCreateConnectCluster() throws InterruptedException {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("ns1-connectCluster")
                .namespace("ns1")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url(getConnectUrl())
                .username("")
                .password("")
                .aes256Key("aes256Key")
                .aes256Salt("aes256Salt")
                .aes256Format("%s")
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/connect-clusters")
                .bearerAuth(token)
                .body(connectCluster));

        HttpResponse<List<ConnectCluster>> connectClusters = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                        .create(HttpMethod.GET, "/api/namespaces/ns1/connect-clusters")
                        .bearerAuth(token), Argument.listOf(ConnectCluster.class));

        assertEquals(1, connectClusters.getBody().get().size());
        assertEquals("ns1-connectCluster", connectClusters.getBody().get().get(0).getMetadata().getName());

        List<String> passwordsToVault = List.of("password1", "password2");

        HttpResponse<List<VaultResponse>> vaultResponse = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                    .create(HttpMethod.POST, "/api/namespaces/ns1/connect-clusters/ns1-connectCluster/vaults")
                    .bearerAuth(token)
                    .body(passwordsToVault), Argument.listOf(VaultResponse.class));

        assertEquals(2, vaultResponse.getBody().get().size());
        assertEquals("password1", vaultResponse.getBody().get().get(0).getSpec().getClearText());
        assertEquals("password2", vaultResponse.getBody().get().get(1).getSpec().getClearText());

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/connect-clusters?name=ns1*")
                .bearerAuth(token));

        connectClusters = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                        .create(HttpMethod.GET, "/api/namespaces/ns1/connect-clusters")
                        .bearerAuth(token), Argument.listOf(ConnectCluster.class));

        assertEquals(0, connectClusters.getBody().get().size());
    }
}
