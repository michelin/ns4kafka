package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.integration.TopicIntegrationTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.model.AccessControlEntry.Permission;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.service.executor.AccessControlEntryAsyncExecutor;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class StreamIntegrationTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    @Inject
    List<AccessControlEntryAsyncExecutor> aceAsyncExecutorList;

    private String token;

    @BeforeAll
    void init() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("nskafkastream")
                .cluster("test-cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        AccessControlEntry acl1 = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("nskafkastream-acl-topic")
                .build())
            .spec(AccessControlEntrySpec.builder()
                .resourceType(ResourceType.TOPIC)
                .resource("kstream-")
                .resourcePatternType(ResourcePatternType.PREFIXED)
                .permission(Permission.OWNER)
                .grantedTo("nskafkastream")
                .build())
            .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<BearerAccessRefreshToken> response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(namespace));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/nskafkastream/acls")
                .bearerAuth(token)
                .body(acl1));

        AccessControlEntry acl2 = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("nskafkastream-acl-group")
                .build())
            .spec(AccessControlEntrySpec.builder()
                .resourceType(ResourceType.GROUP)
                .resource("kstream-")
                .resourcePatternType(ResourcePatternType.PREFIXED)
                .permission(Permission.OWNER)
                .grantedTo("nskafkastream")
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/nskafkastream/acls")
                .bearerAuth(token)
                .body(acl2));
    }

    @Test
    void shouldVerifyCreationOfAcls() throws InterruptedException, ExecutionException {
        KafkaStream stream = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("kstream-test")
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/nskafkastream/streams")
                .bearerAuth(token)
                .body(stream));

        // Force ACL Sync
        aceAsyncExecutorList.forEach(AccessControlEntryAsyncExecutor::run);
        Admin kafkaClient = getAdminClient();

        var aclTopic = kafkaClient.describeAcls(new AclBindingFilter(
                new ResourcePatternFilter(
                    org.apache.kafka.common.resource.ResourceType.TOPIC,
                    stream.getMetadata().getName(),
                    PatternType.PREFIXED),
                AccessControlEntryFilter.ANY))
            .values()
            .get();

        var aclTransactionalId = kafkaClient.describeAcls(
            new AclBindingFilter(
                new ResourcePatternFilter(
                    org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                    stream.getMetadata().getName(),
                    PatternType.PREFIXED
                ),
                AccessControlEntryFilter.ANY
            ))
            .values()
            .get();

        assertEquals(2, aclTopic.size());
        assertTrue(aclTopic
            .stream()
            .allMatch(aclBinding -> List.of(AclOperation.CREATE, AclOperation.DELETE)
                .contains(aclBinding.entry().operation())));

        assertEquals(1, aclTransactionalId.size());
        assertTrue(aclTransactionalId.stream().findFirst().isPresent());
        assertEquals(AclOperation.WRITE, aclTransactionalId.stream().findFirst().get().entry().operation());
    }
}
