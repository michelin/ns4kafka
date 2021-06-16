package com.michelin.ns4kafka.integration;

import com.fasterxml.jackson.annotation.JsonProperty;
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


@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class TopicTest extends AbstractIntegrationTest {

    @Inject
    @Client("/")
    RxHttpClient client;

    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutorList;

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

        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(topicFirstCreate)).blockingFirst();

        //force Topic Sync
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
        valueToVerify.forEach(entry -> {
            Assertions.assertEquals(config.get(entry.name()), entry.value());
        });
    }


    @Test
    void unauthorizedModifications() throws InterruptedException {

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
        Assertions.assertEquals(exception.getMessage(),  "Validation failed: [Invalid value ns1-topicToModify for name: Namespace not OWNER of this topic]");

        // Compare spec of the topics and assure there is no change
        Assertions.assertEquals(topicToModify.getSpec(),client.retrieve(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns1/topics/ns1-topicToModify").bearerAuth(token), Topic.class ).blockingFirst().getSpec());
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class BearerAccessRefreshToken {
        private String username;
        private Collection<String> roles;

        @JsonProperty("access_token")
        private String accessToken;

        @JsonProperty("token_type")
        private String tokenType;

        @JsonProperty("expires_in")
        private Integer expiresIn;
    }


}
