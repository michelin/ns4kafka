package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.models.connector.ChangeConnectorState;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorSpecs;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorStateInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ServerInfo;
import com.michelin.ns4kafka.services.executors.ConnectorAsyncExecutor;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class ConnectTest extends AbstractIntegrationConnectTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    private HttpClient connectClient;

    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutorList;

    @Inject
    List<ConnectorAsyncExecutor> connectorAsyncExecutorList;

    private String token;

    @BeforeAll
    void init() {
        connectClient = applicationContext.createBean(HttpClient.class, connectContainer.getUrl());

        Namespace ns1 = Namespace.builder()
            .metadata(ObjectMeta.builder()
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
            ns4KafkaClient.toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();

        ns4KafkaClient.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1));
        ns4KafkaClient.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1));

        AccessControlEntry aclConnect = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-acl")
                .namespace("ns1")
                .build())
            .spec(AccessControlEntrySpec.builder()
                .resourceType(AclType.CONNECT)
                .resource("ns1-")
                .resourcePatternType(ResourcePatternType.PREFIXED)
                .permission(Permission.OWNER)
                .grantedTo("ns1")
                .build())
            .build();

        ns4KafkaClient.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls").bearerAuth(token).body(aclConnect));

        AccessControlEntry aclTopic = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-acl-topic")
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

        ns4KafkaClient.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls").bearerAuth(token).body(aclTopic));
    }

    @Test
    void createConnect() {
        ServerInfo actual = connectClient.toBlocking().retrieve(HttpRequest.GET("/"), ServerInfo.class);
        assertEquals("7.4.1-ccs", actual.version());
    }

    @Test
    void createNamespaceWithoutConnect() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("ns-without-connect")
                .cluster("test-cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("user-without-connect")
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        assertDoesNotThrow(() -> ns4KafkaClient.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns)));
    }

    /**
     * Validate connector deployment
     * Deploy a topic and connectors and assert the deployments worked.
     * The test asserts null/empty connector properties are deployed.
     *
     * @throws InterruptedException  Any interrupted exception
     * @throws MalformedURLException Any malformed URL exception
     */
    @Test
    void deployConnectors() throws InterruptedException, MalformedURLException {
        Map<String, String> connectorSpecs = new HashMap<>();
        connectorSpecs.put("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector");
        connectorSpecs.put("tasks.max", "1");
        connectorSpecs.put("topics", "ns1-to1");
        connectorSpecs.put("file", null);

        Connector connectorWithNullParameter = Connector.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-connectorWithNullParameter")
                .namespace("ns1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("test-connect")
                .config(connectorSpecs)
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-to1")
                .namespace("ns1")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        ns4KafkaClient.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics").bearerAuth(token).body(topic));
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);
        ns4KafkaClient.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token)
                .body(connectorWithNullParameter));

        Connector connectorWithEmptyParameter = Connector.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-connectorWithEmptyParameter")
                .namespace("ns1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("test-connect")
                .config(Map.of(
                    "connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector",
                    "tasks.max", "1",
                    "topics", "ns1-to1",
                    "file", ""
                ))
                .build())
            .build();

        ns4KafkaClient.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token)
                .body(connectorWithEmptyParameter));

        Connector connectorWithFillParameter = Connector.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-connectorWithFillParameter")
                .namespace("ns1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("test-connect")
                .config(Map.of(
                    "connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector",
                    "tasks.max", "1",
                    "topics", "ns1-to1",
                    "file", "test"
                ))
                .build())
            .build();

        ns4KafkaClient.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token)
                .body(connectorWithFillParameter));

        Flux.fromIterable(connectorAsyncExecutorList).flatMap(ConnectorAsyncExecutor::runHealthCheck).subscribe();
        Flux.fromIterable(connectorAsyncExecutorList).flatMap(ConnectorAsyncExecutor::run).subscribe();

        Thread.sleep(2000);

        // "File" property is present, but null
        ConnectorInfo actualConnectorWithNullParameter = connectClient.toBlocking()
            .retrieve(HttpRequest.GET("/connectors/ns1-connectorWithNullParameter"), ConnectorInfo.class);

        assertTrue(actualConnectorWithNullParameter.config().containsKey("file"));
        Assertions.assertNull(actualConnectorWithNullParameter.config().get("file"));

        // "File" property is present, but empty
        ConnectorInfo actualConnectorWithEmptyParameter = connectClient.toBlocking()
            .retrieve(HttpRequest.GET("/connectors/ns1-connectorWithEmptyParameter"), ConnectorInfo.class);

        assertTrue(actualConnectorWithEmptyParameter.config().containsKey("file"));
        assertTrue(actualConnectorWithEmptyParameter.config().get("file").isEmpty());

        // "File" property is present
        ConnectorInfo actualConnectorWithFillParameter = connectClient.toBlocking()
            .retrieve(HttpRequest.GET("/connectors/ns1-connectorWithFillParameter"), ConnectorInfo.class);

        assertTrue(actualConnectorWithFillParameter.config().containsKey("file"));
        assertEquals("test", actualConnectorWithFillParameter.config().get("file"));
    }

    @Test
    void updateConnectorsWithNullProperty() throws InterruptedException, MalformedURLException {
        ConnectorSpecs connectorSpecs = ConnectorSpecs.builder()
            .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector",
                "tasks.max", "1",
                "topics", "ns1-to1",
                "file", "test"
            ))
            .build();

        Map<String, String> updatedConnectorSpecs = new HashMap<>();
        updatedConnectorSpecs.put("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector");
        updatedConnectorSpecs.put("tasks.max", "1");
        updatedConnectorSpecs.put("topics", "ns1-to1");
        updatedConnectorSpecs.put("file", null);

        HttpResponse<ConnectorInfo> connectorInfo = connectClient.toBlocking()
            .exchange(HttpRequest.PUT("/connectors/ns1-connector/config", connectorSpecs), ConnectorInfo.class);

        // "File" property is present and fill
        assertTrue(connectorInfo.getBody().isPresent());
        assertTrue(connectorInfo.getBody().get().config().containsKey("file"));
        assertEquals("test", connectorInfo.getBody().get().config().get("file"));

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-to1")
                .namespace("ns1")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        ns4KafkaClient.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics").bearerAuth(token).body(topic));

        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        Connector updateConnector = Connector.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-connector")
                .namespace("ns1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("test-connect")
                .config(updatedConnectorSpecs)
                .build())
            .build();

        ns4KafkaClient.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token)
                .body(updateConnector));

        Flux.fromIterable(connectorAsyncExecutorList).flatMap(ConnectorAsyncExecutor::runHealthCheck).subscribe();
        Flux.fromIterable(connectorAsyncExecutorList).flatMap(ConnectorAsyncExecutor::run).subscribe();

        Thread.sleep(2000);

        ConnectorInfo actualConnector = connectClient.toBlocking()
            .retrieve(HttpRequest.GET("/connectors/ns1-connector"), ConnectorInfo.class);

        // "File" property is present, but null
        assertTrue(actualConnector.config().containsKey("file"));
        Assertions.assertNull(actualConnector.config().get("file"));
    }

    @Test
    void restartConnector() throws InterruptedException {
        Topic to = Topic.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-to1")
                .namespace("ns1")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        Connector co = Connector.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-co1")
                .namespace("ns1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("test-connect")
                .config(Map.of(
                    "connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector",
                    "tasks.max", "1",
                    "topics", "ns1-to1"
                ))
                .build())
            .build();

        ns4KafkaClient.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics").bearerAuth(token).body(to));
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);
        ns4KafkaClient.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token).body(co));

        Flux.fromIterable(connectorAsyncExecutorList).flatMap(ConnectorAsyncExecutor::runHealthCheck).subscribe();
        Flux.fromIterable(connectorAsyncExecutorList).flatMap(ConnectorAsyncExecutor::run).subscribe();

        Thread.sleep(2000);

        ChangeConnectorState restartState = ChangeConnectorState.builder()
            .metadata(ObjectMeta.builder().name("ns1-co1").build())
            .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder()
                .action(ChangeConnectorState.ConnectorAction.restart).build())
            .build();

        HttpResponse<ChangeConnectorState> actual = ns4KafkaClient.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors/ns1-co1/change-state").bearerAuth(token)
                .body(restartState), ChangeConnectorState.class);
        assertEquals(HttpStatus.OK, actual.status());
    }

    @Test
    void pauseAndResumeConnector() throws MalformedURLException, InterruptedException {
        Topic to = Topic.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-to1")
                .namespace("ns1")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        Connector co = Connector.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1-co2")
                .namespace("ns1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("test-connect")
                .config(Map.of(
                    "connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector",
                    "tasks.max", "3",
                    "topics", "ns1-to1"
                ))
                .build())
            .build();

        ns4KafkaClient.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics").bearerAuth(token).body(to));
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);
        ns4KafkaClient.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token).body(co));

        Flux.fromIterable(connectorAsyncExecutorList).flatMap(ConnectorAsyncExecutor::runHealthCheck).subscribe();
        Flux.fromIterable(connectorAsyncExecutorList).flatMap(ConnectorAsyncExecutor::run).subscribe();

        Thread.sleep(2000);

        // pause the connector
        ChangeConnectorState pauseState = ChangeConnectorState.builder()
            .metadata(ObjectMeta.builder().name("ns1-co2").build())
            .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder()
                .action(ChangeConnectorState.ConnectorAction.pause).build())
            .build();
        ns4KafkaClient.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors/ns1-co2/change-state").bearerAuth(token)
                .body(pauseState));

        Thread.sleep(2000);

        ConnectorStateInfo actual = connectClient.toBlocking()
            .retrieve(HttpRequest.GET("/connectors/ns1-co2/status"), ConnectorStateInfo.class);
        assertEquals("PAUSED", actual.connector().getState());
        assertEquals("PAUSED", actual.tasks().get(0).getState());
        assertEquals("PAUSED", actual.tasks().get(1).getState());
        assertEquals("PAUSED", actual.tasks().get(2).getState());

        // resume the connector
        ChangeConnectorState resumeState = ChangeConnectorState.builder()
            .metadata(ObjectMeta.builder().name("ns1-co2").build())
            .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder()
                .action(ChangeConnectorState.ConnectorAction.resume).build())
            .build();
        ns4KafkaClient.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors/ns1-co2/change-state").bearerAuth(token)
                .body(resumeState));

        Thread.sleep(2000);

        // verify resumed directly on connect cluster
        actual = connectClient.toBlocking()
            .retrieve(HttpRequest.GET("/connectors/ns1-co2/status"), ConnectorStateInfo.class);
        assertEquals("RUNNING", actual.connector().getState());
        assertEquals("RUNNING", actual.tasks().get(0).getState());
        assertEquals("RUNNING", actual.tasks().get(1).getState());
        assertEquals("RUNNING", actual.tasks().get(2).getState());
    }
}
