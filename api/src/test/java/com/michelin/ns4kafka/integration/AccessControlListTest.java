package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.integration.TopicTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.models.AccessControlEntry.Permission;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.RoleBinding.*;
import com.michelin.ns4kafka.services.executors.AccessControlEntryAsyncExecutor;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.rxjava3.http.client.Rx3HttpClient;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class AccessControlListTest extends AbstractIntegrationTest {

    @Inject
    @Client("/")
    Rx3HttpClient client;

    @Inject
    List<AccessControlEntryAsyncExecutor> accessControlEntryAsyncExecutorList;

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
    void createTopicReadACL() throws InterruptedException, ExecutionException {

        AccessControlEntry aclTopic = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-acl")
                      .namespace("ns1")
                      .build())
            .spec(AccessControlEntrySpec.builder()
                  .resourceType(ResourceType.TOPIC)
                  .resource("ns1-")
                  .resourcePatternType(ResourcePatternType.PREFIXED)
                  .permission(Permission.READ)
                  .grantedTo("ns1")
                  .build())
            .build();

        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(aclTopic)).blockingFirst();

        //force ACL Sync
        accessControlEntryAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();

        AclBindingFilter user1Filter = new AclBindingFilter(
                ResourcePatternFilter.ANY,
                new AccessControlEntryFilter("User:user1", null, AclOperation.ANY, AclPermissionType.ANY));
        Collection<AclBinding> results = kafkaClient.describeAcls(user1Filter).values().get();

        AclBinding expected = new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC, "ns1-", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry("User:user1", "*", AclOperation.READ, AclPermissionType.ALLOW));

        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(expected, results.stream().findFirst().get());

        // DELETE the ACL and verify
        client.exchange(HttpRequest.create(HttpMethod.DELETE,"/api/namespaces/ns1/acls/ns1-acl").bearerAuth(token).body(aclTopic)).blockingFirst();

        accessControlEntryAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(user1Filter).values().get();

        Assertions.assertTrue(results.isEmpty());
    }

    @Test
    void createConnectOwnerACL() throws InterruptedException, ExecutionException {

        AccessControlEntry aclTopic = AccessControlEntry.builder()
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

        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(aclTopic)).blockingFirst();

        //force ACL Sync
        accessControlEntryAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();

        AclBindingFilter user1Filter = new AclBindingFilter(
                ResourcePatternFilter.ANY,
                new AccessControlEntryFilter("User:user1", null, AclOperation.ANY, AclPermissionType.ANY));
        Collection<AclBinding> results = kafkaClient.describeAcls(user1Filter).values().get();

        AclBinding expected = new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.GROUP, "connect-ns1-", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry("User:user1", "*", AclOperation.READ, AclPermissionType.ALLOW));

        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(expected, results.stream().findFirst().get());

        // DELETE the ACL and verify
        client.exchange(HttpRequest.create(HttpMethod.DELETE,"/api/namespaces/ns1/acls/ns1-acl").bearerAuth(token).body(aclTopic)).blockingFirst();

        accessControlEntryAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(user1Filter).values().get();

        Assertions.assertTrue(results.isEmpty());
    }

    @Test
    void createStreamACL() throws InterruptedException, ExecutionException {
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
        AccessControlEntry aclGroup = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-acl-group")
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

        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(aclTopic)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/acls").bearerAuth(token).body(aclGroup)).blockingFirst();

        //force ACL Sync
        accessControlEntryAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();

        AclBindingFilter user1Filter = new AclBindingFilter(
                ResourcePatternFilter.ANY,
                new AccessControlEntryFilter("User:user1", null, AclOperation.ANY, AclPermissionType.ANY));
        Collection<AclBinding> results = kafkaClient.describeAcls(user1Filter).values().get();

        // Topic ns1- READ
        // Topic ns1- WRITE
        // Group ns1- READ

        AclBinding ac1 = new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC, "ns1-", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry("User:user1", "*", AclOperation.READ, AclPermissionType.ALLOW));
        AclBinding ac2 = new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC, "ns1-", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry("User:user1", "*", AclOperation.WRITE, AclPermissionType.ALLOW));
        AclBinding ac3 = new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.GROUP, "ns1-", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry("User:user1", "*", AclOperation.READ, AclPermissionType.ALLOW));

        Assertions.assertEquals(3, results.size());
        Assertions.assertTrue(results.containsAll(List.of(ac1, ac2, ac3)));


        KafkaStream stream = KafkaStream.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-stream1")
                        .namespace("ns1")
                        .build())
                .build();

        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/streams").bearerAuth(token).body(stream)).blockingFirst();

        //force ACL Sync
        accessControlEntryAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(user1Filter).values().get();

        // Topic ns1- READ
        // Topic ns1- WRITE
        // Group ns1- READ
        // Topic ns1-stream1 CREATE
        // Topic ns1-stream1 DELETE
        AclBinding ac4 = new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC, "ns1-stream1", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry("User:user1", "*", AclOperation.CREATE, AclPermissionType.ALLOW));
        AclBinding ac5 = new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC, "ns1-stream1", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry("User:user1", "*", AclOperation.DELETE, AclPermissionType.ALLOW));
        AclBinding ac6 = new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID, "ns1-stream1", PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry("User:user1", "*", AclOperation.WRITE, AclPermissionType.ALLOW));

        Assertions.assertEquals(6, results.size());
        Assertions.assertTrue(results.containsAll(List.of(ac1, ac2, ac3, ac4, ac5, ac6)));


        // DELETE the Stream & ACL and verify
        client.exchange(HttpRequest.create(HttpMethod.DELETE,"/api/namespaces/ns1/streams/ns1-stream1").bearerAuth(token).body(aclTopic)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.DELETE,"/api/namespaces/ns1/acls/ns1-acl-topic").bearerAuth(token).body(aclTopic)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.DELETE,"/api/namespaces/ns1/acls/ns1-acl-group").bearerAuth(token).body(aclTopic)).blockingFirst();

        accessControlEntryAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(user1Filter).values().get();

        Assertions.assertTrue(results.isEmpty());
    }
}
