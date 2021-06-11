package com.michelin.ns4kafka.integration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.michelin.ns4kafka.integration.NamespaceReadAccessToTopic.BearerAccessRefreshToken;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.models.AccessControlEntry.Permission;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourceType;
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
import com.michelin.ns4kafka.models.Topic.TopicSpec;
import com.michelin.ns4kafka.validation.TopicValidator;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.security.authentication.UsernamePasswordCredentials;

@Testcontainers
public class VerifyIfTopicExistsOutsideOfNs4kafka extends AbstractIntegrationTest{

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
                  .topicValidator(TopicValidator.makeDefault())
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
                  .replicationFactor(3)
                  .configs(Map.of("cleanup.policy", "delete",
                                  "min.insync.replicas", "2",
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

        Admin kafkaClient = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()));
        System.out.println(kafkaClient.describeTopics(List.of("ns1-topic1")).all().get());
        List<TopicPartitionInfo> topicPartitionInfos = kafkaClient.describeTopics(List.of("ns1-topic1")).all().get()
            .get("ns1-topic1").partitions();
        Assertions.assertEquals(t1.getSpec().getPartitions(), topicPartitionInfos.size());
        topicPartitionInfos.stream()
            .forEach(topicPartitionInfo -> {
        });
    }

}
