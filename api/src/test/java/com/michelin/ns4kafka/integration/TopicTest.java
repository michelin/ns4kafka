package com.michelin.ns4kafka.integration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.controllers.AkhqClaimProviderController;
import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.models.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.models.AccessControlEntry.Permission;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.RoleBinding.*;
import com.michelin.ns4kafka.models.Topic.TopicSpec;
import com.michelin.ns4kafka.services.TopicService;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class TopicTest extends AbstractIntegrationTest {
    /**
     * The HTTP client
     */
    @Inject
    @Client("/")
    RxHttpClient client;

    /**
     * The topic sync executor
     */
    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutorList;

    /**
     * The topic service
     */
    @Inject
    TopicService topicService;

    /**
     * The Authentication token
     */
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
        HttpResponse<BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class).blockingFirst();

        token = response.getBody().get().getAccessToken();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(ns1acl)).blockingFirst();

        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces").bearerAuth(token).body(ns2)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns2/role-bindings").bearerAuth(token).body(rb2)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns2/acls").bearerAuth(token).body(ns2acl)).blockingFirst();
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

        AkhqClaimProviderController.AKHQClaimResponse response =  client.retrieve(
                HttpRequest.POST("/akhq-claim", akhqClaimRequest),
                AkhqClaimProviderController.AKHQClaimResponse.class).blockingFirst();

        Assertions.assertLinesMatch(
                List.of(
                        "topic/read",
                        "topic/data/read",
                        "group/read",
                        "registry/read",
                        "connect/read",
                        "connect/state/update"
                ),
                response.getRoles());

        Assertions.assertEquals(1, response.getAttributes().get("topicsFilterRegexp").size());

        Assertions.assertLinesMatch(List.of("^\\Qns1-\\E.*$"), response.getAttributes().get("topicsFilterRegexp"));
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

        var response = client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topicFirstCreate)).blockingFirst();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));

        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();
        System.out.println(kafkaClient.describeTopics(List.of("ns1-topicFirstCreate")).all().get());
        List<TopicPartitionInfo> topicPartitionInfos = kafkaClient.describeTopics(List.of("ns1-topicFirstCreate")).all().get()
            .get("ns1-topicFirstCreate").partitions();
        Assertions.assertEquals(topicFirstCreate.getSpec().getPartitions(), topicPartitionInfos.size());

        Map<String, String> config = topicFirstCreate.getSpec().getConfigs();
        Set<String> configKey = config.keySet();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,"ns1-topicFirstCreate");
        List<ConfigEntry> valueToVerify = kafkaClient.describeConfigs(List.of(configResource)).all().get().get(configResource).entries().stream()
            .filter(e -> configKey.contains(e.name()))
            .collect(Collectors.toList());

        Assertions.assertEquals(config.size(), valueToVerify.size());
        valueToVerify.forEach(entry -> Assertions.assertEquals(config.get(entry.name()), entry.value()));
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

        var response = client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topic2Create)).blockingFirst();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));

        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        response = client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topic2Create)).blockingFirst();
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));

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

        response = client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topic2Update)).blockingFirst();
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));

        //force Topic Sync
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();
        System.out.println(kafkaClient.describeTopics(List.of("ns1-topic2Create")).all().get());
        List<TopicPartitionInfo> topicPartitionInfos = kafkaClient.describeTopics(List.of("ns1-topic2Create")).all().get()
            .get("ns1-topic2Create").partitions();
        // verify partition of the updated topic
        Assertions.assertEquals(topic2Update.getSpec().getPartitions(), topicPartitionInfos.size());

        // verify config of the updated topic
        Map<String, String> config = topic2Update.getSpec().getConfigs();
        Set<String> configKey = config.keySet();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,"ns1-topic2Create");
        List<ConfigEntry> valueToVerify = kafkaClient.describeConfigs(List.of(configResource)).all().get().get(configResource).entries().stream()
            .filter(e -> configKey.contains(e.name()))
            .collect(Collectors.toList());

        Assertions.assertEquals(config.size(), valueToVerify.size());
        valueToVerify.forEach(entry -> {
            Assertions.assertEquals(config.get(entry.name()), entry.value());
        });
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

        HttpClientResponseException exception = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics")
                        .bearerAuth(token)
                        .body(topicFirstCreate))
                        .blockingFirst());

        Assertions.assertEquals("Invalid Resource", exception.getMessage());
        Assertions.assertEquals("topic.metadata.name: must match \"^[a-zA-Z0-9_.-]+$\"", exception.getResponse().getBody(Status.class).get().getDetails().getCauses().get(0));

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

        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(aclns1Tons2)).blockingFirst();

        Assertions.assertEquals(HttpStatus.OK, client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topicToModify)).blockingFirst().getStatus());
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

        HttpClientResponseException exception = Assertions.assertThrows(HttpClientResponseException.class,() -> client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns2/topics").bearerAuth(token).body(topicToModifyBis)).blockingFirst());
        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());

        // Compare spec of the topics and assure there is no change
        Assertions.assertEquals(topicToModify.getSpec(),client.retrieve(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns1/topics/ns1-topicToModify").bearerAuth(token), Topic.class ).blockingFirst().getSpec());
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

        var response = client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topicToDelete)).blockingFirst();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));

        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        List<DeleteRecordsResponse> deleteRecordsResponse = client.retrieve(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics/ns1-topicToDelete/delete-records").bearerAuth(token), Argument.listOf(DeleteRecordsResponse.class)).blockingFirst();

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

        Assertions.assertEquals(3L, deleteRecordsResponse.size());

        assertNotNull(resultPartition0);
        Assertions.assertEquals(0, resultPartition0.getSpec().getOffset());
        Assertions.assertEquals(0, resultPartition0.getSpec().getPartition());
        Assertions.assertEquals("ns1-topicToDelete", resultPartition0.getSpec().getTopic());

        assertNotNull(resultPartition1);
        Assertions.assertEquals(0, resultPartition1.getSpec().getOffset());
        Assertions.assertEquals(1, resultPartition1.getSpec().getPartition());
        Assertions.assertEquals("ns1-topicToDelete", resultPartition1.getSpec().getTopic());

        assertNotNull(resultPartition2);
        Assertions.assertEquals(0, resultPartition2.getSpec().getOffset());
        Assertions.assertEquals(2, resultPartition2.getSpec().getPartition());
        Assertions.assertEquals("ns1-topicToDelete", resultPartition2.getSpec().getTopic());
   }

    /**
     * Validate records deletion on compacted topic
     */
    @Test
    void testDeleteRecordsCompactTopic() {
        Topic topicToDelete = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-topicToDelete")
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

        var response = client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topicToDelete)).blockingFirst();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));

        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics/ns1-topicToDelete/delete-records")
                        .bearerAuth(token))
                        .blockingFirst());

        Assertions.assertEquals(1, exception.getValidationErrors().size());
        Assertions.assertEquals("Cannot delete records on a compacted topic. Please delete and recreate the topic.", exception.getValidationErrors().get(0));
    }

    /**
     * Bearer token class
     */
    @Data
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
