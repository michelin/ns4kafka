package com.michelin.ns4kafka.integration;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.michelin.ns4kafka.integration.TopicTest.BearerAccessRefreshToken;
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
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.TopicValidator;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class AccessControlListTest extends AbstractIntegrationTest {

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

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class).blockingFirst();

        token = response.getBody().get().getAccessToken();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1)).blockingFirst();
    }

    @Test
    void createACL() throws InterruptedException, ExecutionException {

        AccessControlEntry aclTopic = AccessControlEntry.builder()
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

        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(aclTopic)).blockingFirst();

        List<Map<String, Object>> aclTopicSaved = client.retrieve(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns1/acls").bearerAuth(token), List.class).blockingFirst();
        System.out.println(aclTopicSaved);
        AccessControlEntrySpec specExpected = aclTopic.getSpec();
        Assertions.assertNotNull(aclTopicSaved.get(0));

        Assertions.assertEquals(HttpStatus.NO_CONTENT , client.exchange(HttpRequest.create(HttpMethod.DELETE,"/api/namespaces/ns1/acls/ns1-acl").bearerAuth(token)).blockingFirst().getStatus());
    }
}
