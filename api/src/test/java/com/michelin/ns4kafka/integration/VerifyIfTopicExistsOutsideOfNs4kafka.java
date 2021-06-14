package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.integration.NamespaceReadAccessToTopic.BearerAccessRefreshToken;
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
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class VerifyIfTopicExistsOutsideOfNs4kafka extends AbstractIntegrationTest{

    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutorList;
    @Inject
    @Client("/")
    RxHttpClient client;
    @Test
    void verifyIfTopicExistsOutsideOfNs4kafka() throws InterruptedException, ExecutionException {

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

        AccessControlEntry acl1ns1 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-acl1")
                      .namespace("ns1")
                      .build())
            .spec(AccessControlEntrySpec.builder()
                  .resourceType(ResourceType.GROUP)
                  .resource("ns1-")
                  .resourcePatternType(ResourcePatternType.PREFIXED)
                  .permission(Permission.OWNER)
                  .grantedTo("ns1")
                  .build())
            .build();

        AccessControlEntry acl2ns1 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-acl2")
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

        Topic t1 = Topic.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-topic1")
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

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class).blockingFirst();
        String token = response.getBody().get().getAccessToken();

        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces").bearerAuth(token).body(ns1));
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(acl1ns1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(acl2ns1)).blockingFirst();

        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/topics").bearerAuth(token).body(t1)).blockingFirst();

        //force Topic Sync
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        Admin kafkaClient = KafkaCluster.getAdminClient();
        System.out.println(kafkaClient.describeTopics(List.of("ns1-topic1")).all().get());
        List<TopicPartitionInfo> topicPartitionInfos = kafkaClient.describeTopics(List.of("ns1-topic1")).all().get()
            .get("ns1-topic1").partitions();
        Assertions.assertEquals(t1.getSpec().getPartitions(), topicPartitionInfos.size());

        Map<String, String> config = t1.getSpec().getConfigs();
        Set<String> configKey = config.keySet();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,"ns1-topic1");
        List<ConfigEntry> valueToVerify = kafkaClient.describeConfigs(List.of(configResource)).all().get().get(configResource).entries().stream()
            .filter(e -> configKey.contains(e.name()))
            .collect(Collectors.toList());

        Assertions.assertEquals(config.size(), valueToVerify.size());
        valueToVerify.forEach(entry -> {
            Assertions.assertEquals(config.get(entry.name()), entry.value());
        });
    }
}
