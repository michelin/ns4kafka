package com.michelin.ns4kafka.integration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.controllers.AkhqClaimProviderController;
import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.models.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.models.AccessControlEntry.Permission;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.RoleBinding.*;
import com.michelin.ns4kafka.models.Topic.TopicSpec;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.serde.annotation.Serdeable;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class TopicTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    HttpClient client;

    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutorList;

    private String token;

    /**
     * Init all integration tests
     */
    @BeforeAll
    void init(){
        Namespace ns1 = Namespace.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1")
                      .cluster("test-cluster")
                    .labels(Map.of("support-group", "LDAP-GROUP-1"))
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

        Namespace ns2 = Namespace.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns2")
                      .cluster("test-cluster")
                      .build())
            .spec(NamespaceSpec.builder()
                  .kafkaUser("user2")
                  .connectClusters(List.of("test-connect"))
                  .topicValidator(TopicValidator.makeDefaultOneBroker())
                  .build())
            .build();

        RoleBinding rb2 = RoleBinding.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns2-rb")
                      .namespace("ns2")
                      .build())
            .spec(RoleBindingSpec.builder()
                  .role(Role.builder()
                        .resourceTypes(List.of("topics", "acls"))
                        .verbs(List.of(Verb.POST, Verb.GET))
                        .build())
                  .subject(Subject.builder()
                           .subjectName("group2")
                           .subjectType(SubjectType.GROUP)
                           .build())
                  .build())
            .build();

        AccessControlEntry ns2acl = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns2-acl")
                      .namespace("ns2")
                      .build())
            .spec(AccessControlEntrySpec.builder()
                  .resourceType(ResourceType.TOPIC)
                  .resource("ns2-")
                  .resourcePatternType(ResourcePatternType.PREFIXED)
                  .permission(Permission.OWNER)
                  .grantedTo("ns2")
                  .build())
            .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<BearerAccessRefreshToken> response = client.toBlocking().exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();

        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1));
        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1));
        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(ns1acl));

        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces").bearerAuth(token).body(ns2));
        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns2/role-bindings").bearerAuth(token).body(rb2));
        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns2/acls").bearerAuth(token).body(ns2acl));
    }

    /**
     * Validate AKHQ claims
     */
    @Test
    void akhqClaim(){
        AkhqClaimProviderController.AKHQClaimRequest akhqClaimRequest = AkhqClaimProviderController.AKHQClaimRequest.builder()
                .username("test")
                .groups(List.of("LDAP-GROUP-1"))
                .providerName("LDAP")
                .build();

        AkhqClaimProviderController.AKHQClaimResponse response = client.toBlocking().retrieve(
                HttpRequest.POST("/akhq-claim", akhqClaimRequest),
                AkhqClaimProviderController.AKHQClaimResponse.class);

        assertLinesMatch(
                List.of(
                        "topic/read",
                        "topic/data/read",
                        "group/read",
                        "registry/read",
                        "connect/read",
                        "connect/state/update"
                ),
                response.getRoles());

        assertEquals(1, response.getAttributes().get("topicsFilterRegexp").size());

        assertLinesMatch(List.of("^\\Qns1-\\E.*$"), response.getAttributes().get("topicsFilterRegexp"));
    }

    /**
     * Validate topic creation
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     */
    @Test
    void createTopic() throws InterruptedException, ExecutionException {
        Topic topicFirstCreate = Topic.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-topicFirstCreate")
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

        var response = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topicFirstCreate));
        assertEquals("created", response.header("X-Ns4kafka-Result"));

        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();
        System.out.println(kafkaClient.describeTopics(List.of("ns1-topicFirstCreate")).all().get());
        List<TopicPartitionInfo> topicPartitionInfos = kafkaClient.describeTopics(List.of("ns1-topicFirstCreate")).all().get()
            .get("ns1-topicFirstCreate").partitions();
        assertEquals(topicFirstCreate.getSpec().getPartitions(), topicPartitionInfos.size());

        Map<String, String> config = topicFirstCreate.getSpec().getConfigs();
        Set<String> configKey = config.keySet();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,"ns1-topicFirstCreate");
        List<ConfigEntry> valueToVerify = kafkaClient.describeConfigs(List.of(configResource)).all().get().get(configResource).entries().stream()
            .filter(e -> configKey.contains(e.name()))
                .toList();

        assertEquals(config.size(), valueToVerify.size());
        valueToVerify.forEach(entry -> assertEquals(config.get(entry.name()), entry.value()));
    }

    /**
     * Validate topic update
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     */
    @Test
    void updateTopic() throws InterruptedException, ExecutionException {
        Topic topic2Create = Topic.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-topic2Create")
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

        var response = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topic2Create));
        assertEquals("created", response.header("X-Ns4kafka-Result"));

        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        response = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topic2Create));
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));

        Topic topic2Update = Topic.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-topic2Create")
                      .namespace("ns1")
                      .build())
            .spec(TopicSpec.builder()
                  .partitions(3)
                  .replicationFactor(1)
                  .configs(Map.of("cleanup.policy", "delete",
                                  "min.insync.replicas", "1",
                                  "retention.ms", "70000"))//This line was changed
                  .build())
            .build();

        response = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topic2Update));
        assertEquals("changed", response.header("X-Ns4kafka-Result"));

        //force Topic Sync
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();
        System.out.println(kafkaClient.describeTopics(List.of("ns1-topic2Create")).all().get());
        List<TopicPartitionInfo> topicPartitionInfos = kafkaClient.describeTopics(List.of("ns1-topic2Create")).all().get()
            .get("ns1-topic2Create").partitions();
        // verify partition of the updated topic
        assertEquals(topic2Update.getSpec().getPartitions(), topicPartitionInfos.size());

        // verify config of the updated topic
        Map<String, String> config = topic2Update.getSpec().getConfigs();
        Set<String> configKey = config.keySet();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,"ns1-topic2Create");
        List<ConfigEntry> valueToVerify = kafkaClient.describeConfigs(List.of(configResource)).all().get().get(configResource).entries().stream()
            .filter(e -> configKey.contains(e.name()))
                .toList();

        assertEquals(config.size(), valueToVerify.size());
        valueToVerify.forEach(entry -> assertEquals(config.get(entry.name()), entry.value()));
    }

    /**
     * Validate topic creation when topic name is invalid
     */
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
                () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics")
                        .bearerAuth(token)
                        .body(topicFirstCreate)));

        assertEquals("Invalid Resource", exception.getMessage());
        assertEquals("topic.metadata.name: must match \"^[a-zA-Z0-9_.-]+$\"", exception.getResponse().getBody(Status.class).get().getDetails().getCauses().get(0));

    }

    /**
     * Validate topic creation when there is no change on it
     */
    @Test
    void updateTopicNoChange() {
        AccessControlEntry aclns1Tons2 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-acltons2")
                      .namespace("ns1")
                      .build())
            .spec(AccessControlEntrySpec.builder()
                  .resourceType(ResourceType.TOPIC)
                  .resource("ns1-")
                  .resourcePatternType(ResourcePatternType.PREFIXED)
                  .permission(Permission.READ)
                  .grantedTo("ns2")
                  .build())
            .build();

        Topic topicToModify = Topic.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-topicToModify")
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

        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(aclns1Tons2));

        assertEquals(HttpStatus.OK, client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topicToModify)).getStatus());
        Topic topicToModifyBis = Topic.builder()
            .metadata(topicToModify.getMetadata())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                                "min.insync.replicas", "1",
                                "retention.ms", "90000"))
                .build())
            .build();

        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,() -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns2/topics").bearerAuth(token).body(topicToModifyBis)));
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());

        // Compare spec of the topics and assure there is no change
        assertEquals(topicToModify.getSpec(), client.toBlocking().retrieve(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns1/topics/ns1-topicToModify").bearerAuth(token), Topic.class ).getSpec());
    }

    /**
     * Validate records deletion on topic
     */
    @Test
    void testDeleteRecords() {
        Topic topicToDelete = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-topicToDelete")
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

        var response = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topicToDelete));
        assertEquals("created", response.header("X-Ns4kafka-Result"));

        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        List<DeleteRecordsResponse> deleteRecordsResponse = client.toBlocking().retrieve(HttpRequest.create(HttpMethod.POST,
                        "/api/namespaces/ns1/topics/ns1-topicToDelete/delete-records").bearerAuth(token),
                Argument.listOf(DeleteRecordsResponse.class));

        DeleteRecordsResponse resultPartition0 = deleteRecordsResponse
                .stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 0)
                .findFirst()
                .orElse(null);

        DeleteRecordsResponse resultPartition1 = deleteRecordsResponse
                .stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 1)
                .findFirst()
                .orElse(null);

        DeleteRecordsResponse resultPartition2 = deleteRecordsResponse
                .stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 2)
                .findFirst()
                .orElse(null);

        assertEquals(3L, deleteRecordsResponse.size());

        assertNotNull(resultPartition0);
        assertEquals(0, resultPartition0.getSpec().getOffset());
        assertEquals(0, resultPartition0.getSpec().getPartition());
        assertEquals("ns1-topicToDelete", resultPartition0.getSpec().getTopic());

        assertNotNull(resultPartition1);
        assertEquals(0, resultPartition1.getSpec().getOffset());
        assertEquals(1, resultPartition1.getSpec().getPartition());
        assertEquals("ns1-topicToDelete", resultPartition1.getSpec().getTopic());

        assertNotNull(resultPartition2);
        assertEquals(0, resultPartition2.getSpec().getOffset());
        assertEquals(2, resultPartition2.getSpec().getPartition());
        assertEquals("ns1-topicToDelete", resultPartition2.getSpec().getTopic());
   }

    /**
     * Validate records deletion on compacted topic
     */
    @Test
    void testDeleteRecordsCompactTopic() {
        Topic topicToDelete = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-compactTopicToDelete")
                        .namespace("ns1")
                        .build())
                .spec(TopicSpec.builder()
                        .partitions(3)
                        .replicationFactor(1)
                        .configs(Map.of("cleanup.policy", "compact",
                                "min.insync.replicas", "1",
                                "retention.ms", "60000"))
                        .build())
                .build();

        var response = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topicToDelete));
        assertEquals("created", response.header("X-Ns4kafka-Result"));

        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics/compactTopicToDelete/delete-records")
                        .bearerAuth(token)));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
    }

    /**
     * Bearer token class
     */
    @Data
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BearerAccessRefreshToken {
        /**
         * The username
         */
        private String username;

        /**
         * The roles
         */
        private Collection<String> roles;

        /**
         * The access token
         */
        @JsonProperty("access_token")
        private String accessToken;

        /**
         * The token type
         */
        @JsonProperty("token_type")
        private String tokenType;

        /**
         * The expires in
         */
        @JsonProperty("expires_in")
        private Integer expiresIn;
    }
}
