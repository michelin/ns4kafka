package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.integration.TopicTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.models.*;
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
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Disabled
@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class StreamTest extends AbstractIntegrationTest {

    @Inject
    @Client("/")
    RxHttpClient client;

    @Inject
    List<AccessControlEntryAsyncExecutor> aceAsyncExecutorList;

    private String token;

    @BeforeAll
    void init(){
        Namespace ns1 = Namespace.builder()
            .metadata(ObjectMeta.builder()
                      .name("nskafkastream")
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
                      .name("nskafkastream-rb")
                      .namespace("nskafkastream")
                      .build())
            .spec(RoleBindingSpec.builder()
                .role(Role.builder()
                        .resourceTypes(List.of("topics", "acls"))
                        .verbs(List.of(Verb.POST, Verb.GET))
                        .build())
                .subject(Subject.builder()
                        .subjectName("groupkafkastream")
                        .subjectType(SubjectType.GROUP)
                        .build())
                  .build())
            .build();

        AccessControlEntry acl = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("nskafkastream-acl")
                      .namespace("ns1")
                      .build())
            .spec(AccessControlEntrySpec.builder()
                  .resourceType(ResourceType.TOPIC)
                  .resource("nskafkastream-")
                  .resourcePatternType(ResourcePatternType.PREFIXED)
                  .permission(Permission.OWNER)
                  .grantedTo("nskafkastream")
                  .build())
            .build();


        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class).blockingFirst();

        token = response.getBody().get().getAccessToken();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/nskafkastream/role-bindings").bearerAuth(token).body(rb1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/nskafkastream/acls").bearerAuth(token).body(acl)).blockingFirst();
    }

    @Test
    void verifyCreationOfAcl() throws InterruptedException, ExecutionException {

        KafkaStream stream = KafkaStream.builder()
                .metadata(ObjectMeta.builder()
                        .name("nskafkastream-streamToTest")
                        .build())
                .build();
        client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/nskafkastream/streams")
                        .bearerAuth(token)
                        .body(stream))
                        .blockingFirst();
        //force ACL Sync
        aceAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);
        Admin kafkaClient = getAdminClient();
        //TODO SecurityDisabledException no authorizer configured
        var aclTopic = kafkaClient.describeAcls(new AclBindingFilter(
                new ResourcePatternFilter(org.apache.kafka.common.resource.ResourceType.TOPIC,
                        stream.getMetadata().getName(),
                        PatternType.PREFIXED),
                AccessControlEntryFilter.ANY)).values().get();

        var aclGroup = kafkaClient.describeAcls(new AclBindingFilter(
                new ResourcePatternFilter(org.apache.kafka.common.resource.ResourceType.GROUP,
                        stream.getMetadata().getName(),
                        PatternType.PREFIXED),
                AccessControlEntryFilter.ANY)).values().get();

        Assertions.assertEquals(2, aclTopic.size());
        Assertions.assertEquals(1, aclGroup.size());
    }
}
