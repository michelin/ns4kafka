package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.integration.TopicTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.models.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.models.AccessControlEntry.Permission;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.RoleBinding.*;
import com.michelin.ns4kafka.models.connector.ChangeConnectorState;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorInfo;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorSpecs;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStateInfo;
import com.michelin.ns4kafka.services.connect.client.entities.ServerInfo;
import com.michelin.ns4kafka.services.executors.ConnectorAsyncExecutor;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.rxjava3.http.client.Rx3HttpClient;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class ConnectTest extends AbstractIntegrationConnectTest {
    @Inject
    @Client("/")
    Rx3HttpClient client;

    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutorList;

    @Inject
    List<ConnectorAsyncExecutor> connectorAsyncExecutorList;

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

        AccessControlEntry aclConnect = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
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

        AccessControlEntry aclTopic = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
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

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class).blockingFirst();

        token = response.getBody().get().getAccessToken();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls").bearerAuth(token).body(aclConnect)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls").bearerAuth(token).body(aclTopic)).blockingFirst();
    }

    /**
     * Validate connector HTTP client creation
     * @throws MalformedURLException Any malformed URL exception
     */
    @Test
    void createConnect() throws MalformedURLException {
        Rx3HttpClient connectCli = Rx3HttpClient.create(new URL(connect.getUrl()));
        ServerInfo actual = connectCli.retrieve(HttpRequest.GET("/"), ServerInfo.class).blockingFirst();
        Assertions.assertEquals("6.2.0-ccs", actual.version());
    }

    /**
     * Validate the namespace creation without connector
     */
    @Test
    void createNamespaceWithoutConnect() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns-without-connect")
                        .cluster("test-cluster")
                        .build())
                .spec(NamespaceSpec.builder()
                        .kafkaUser("user-without-connect")
                        //.connectClusters(List.of("test-connect"))
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        Assertions.assertDoesNotThrow(() -> client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns)).blockingFirst());

    }

    /**
     * Validate connector deployment
     * Deploy a topic and connectors and assert the deployments worked.
     * The test asserts null/empty connector properties are deployed.
     * @throws InterruptedException Any interrupted exception
     * @throws MalformedURLException Any malformed URL exception
     */
    @Test
    void deployConnectors() throws InterruptedException, MalformedURLException {
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

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics").bearerAuth(token).body(to)).blockingFirst();
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token).body(connectorWithNullParameter)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token).body(connectorWithEmptyParameter)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token).body(connectorWithFillParameter)).blockingFirst();
        connectorAsyncExecutorList.forEach(ConnectorAsyncExecutor::run);
        Thread.sleep(2000);

        Rx3HttpClient connectCli = Rx3HttpClient.create(new URL(connect.getUrl()));
        ConnectorInfo actualConnectorWithNullParameter = connectCli.retrieve(HttpRequest.GET("/connectors/ns1-connectorWithNullParameter"), ConnectorInfo.class).blockingFirst();
        ConnectorInfo actualConnectorWithEmptyParameter = connectCli.retrieve(HttpRequest.GET("/connectors/ns1-connectorWithEmptyParameter"), ConnectorInfo.class).blockingFirst();
        ConnectorInfo actualConnectorWithFillParameter = connectCli.retrieve(HttpRequest.GET("/connectors/ns1-connectorWithFillParameter"), ConnectorInfo.class).blockingFirst();

        // "File" property is present, but null
        Assertions.assertTrue(actualConnectorWithNullParameter.config().containsKey("file"));
        Assertions.assertNull(actualConnectorWithNullParameter.config().get("file"));

        // "File" property is present, but empty
        Assertions.assertTrue(actualConnectorWithEmptyParameter.config().containsKey("file"));
        Assertions.assertTrue(actualConnectorWithEmptyParameter.config().get("file").isEmpty());

        // "File" property is present
        Assertions.assertTrue(actualConnectorWithFillParameter.config().containsKey("file"));
        Assertions.assertEquals("test", actualConnectorWithFillParameter.config().get("file"));
    }

    /**
     * Validate connector update when connector is already deployed
     * in the cluster and it is updated with a null property
     * @throws InterruptedException Any interrupted exception
     * @throws MalformedURLException Any malformed URL exception
     */
    @Test
    void updateConnectorsWithNullProperty() throws InterruptedException, MalformedURLException {
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

        Rx3HttpClient connectCli = Rx3HttpClient.create(new URL(connect.getUrl()));
        HttpResponse<ConnectorInfo> connectorInfo = connectCli.exchange(HttpRequest.PUT("/connectors/ns1-connector/config", connectorSpecs), ConnectorInfo.class).blockingFirst();

        // "File" property is present and fill
        Assertions.assertTrue(connectorInfo.getBody().isPresent());
        Assertions.assertTrue(connectorInfo.getBody().get().config().containsKey("file"));
        Assertions.assertEquals("test", connectorInfo.getBody().get().config().get("file"));

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics").bearerAuth(token).body(to)).blockingFirst();
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token).body(updateConnector)).blockingFirst();
        connectorAsyncExecutorList.forEach(ConnectorAsyncExecutor::run);
        Thread.sleep(2000);

        ConnectorInfo actualConnector = connectCli.retrieve(HttpRequest.GET("/connectors/ns1-connector"), ConnectorInfo.class).blockingFirst();

        // "File" property is present, but null
        Assertions.assertTrue(actualConnector.config().containsKey("file"));
        Assertions.assertNull(actualConnector.config().get("file"));
    }

    /**
     * Validate the connector restart
     * @throws InterruptedException Any interrupted exception
     */
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

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics").bearerAuth(token).body(to)).blockingFirst();
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token).body(co)).blockingFirst();
        connectorAsyncExecutorList.forEach(ConnectorAsyncExecutor::run);
        Thread.sleep(2000);

        ChangeConnectorState restartState = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("ns1-co1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.restart).build())
                .build();

        HttpResponse<ChangeConnectorState> actual = client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors/ns1-co1/change-state").bearerAuth(token).body(restartState), ChangeConnectorState.class).blockingFirst();
        Assertions.assertEquals(HttpStatus.OK, actual.status());
    }

    /**
     * Validate connector pause and resume
     * @throws MalformedURLException Any malformed URL exception
     * @throws InterruptedException Any interrupted exception
     */
    @Test
    void pauseAndResumeConnector() throws MalformedURLException, InterruptedException {
        Rx3HttpClient connectCli = Rx3HttpClient.create(new URL(connect.getUrl()));
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

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics").bearerAuth(token).body(to)).blockingFirst();
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors").bearerAuth(token).body(co)).blockingFirst();
        connectorAsyncExecutorList.forEach(ConnectorAsyncExecutor::run);
        Thread.sleep(2000);

        // pause the connector
        ChangeConnectorState pauseState = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("ns1-co2").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.pause).build())
                .build();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors/ns1-co2/change-state").bearerAuth(token).body(pauseState)).blockingFirst();
        Thread.sleep(2000);

        // verify paused directly on connect cluster
        ConnectorStateInfo actual = connectCli.retrieve(HttpRequest.GET("/connectors/ns1-co2/status"), ConnectorStateInfo.class).blockingFirst();
        Assertions.assertEquals("PAUSED", actual.connector().state());
        Assertions.assertEquals("PAUSED", actual.tasks().get(0).state());
        Assertions.assertEquals("PAUSED", actual.tasks().get(1).state());
        Assertions.assertEquals("PAUSED", actual.tasks().get(2).state());

        // resume the connector
        ChangeConnectorState resumeState = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("ns1-co2").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.resume).build())
                .build();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/connectors/ns1-co2/change-state").bearerAuth(token).body(resumeState)).blockingFirst();
        Thread.sleep(2000);

        // verify resumed directly on connect cluster
        actual = connectCli.retrieve(HttpRequest.GET("/connectors/ns1-co2/status"), ConnectorStateInfo.class).blockingFirst();
        Assertions.assertEquals("RUNNING", actual.connector().state());
        Assertions.assertEquals("RUNNING", actual.tasks().get(0).state());
        Assertions.assertEquals("RUNNING", actual.tasks().get(1).state());
        Assertions.assertEquals("RUNNING", actual.tasks().get(2).state());
    }
}
