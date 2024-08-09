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
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.RoleBinding.Role;
import com.michelin.ns4kafka.model.RoleBinding.RoleBindingSpec;
import com.michelin.ns4kafka.model.RoleBinding.Subject;
import com.michelin.ns4kafka.model.RoleBinding.SubjectType;
import com.michelin.ns4kafka.model.RoleBinding.Verb;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class AclIntegrationTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    @Inject
    List<AccessControlEntryAsyncExecutor> accessControlEntryAsyncExecutors;

    private String token;

    @BeforeAll
    void init() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns1")
                .cluster("test-cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        RoleBinding roleBinding = RoleBinding.builder()
            .metadata(Metadata.builder()
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

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<BearerAccessRefreshToken> response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .POST("/login", credentials), BearerAccessRefreshToken.class);

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
                .create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings")
                .bearerAuth(token)
                .body(roleBinding));
    }

    @Test
    void shouldCreateAndDeleteTopicReadAcl() throws InterruptedException, ExecutionException {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-acl-topic")
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

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(accessControlEntry));

        // Force ACLs synchronization
        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();

        AclBindingFilter aclBindingFilter = new AclBindingFilter(
            ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(
                "User:user1",
                null,
                AclOperation.ANY,
                AclPermissionType.ANY
            )
        );

        Collection<AclBinding> results = kafkaClient.describeAcls(aclBindingFilter).values().get();

        AclBinding expected = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-",
                PatternType.PREFIXED),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW
            )
        );

        assertEquals(1, results.size());
        assertTrue(results.stream().findFirst().isPresent());
        assertEquals(expected, results.stream().findFirst().get());

        // DELETE the ACL and verify
        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/acls/ns1-acl-topic")
                .bearerAuth(token));

        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(aclBindingFilter).values().get();

        assertTrue(results.isEmpty());
    }

    @Test
    void shouldCreateAndDeletePublicAcl() throws InterruptedException, ExecutionException {
        AccessControlEntry aclTopicOwner = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-acl-topic-owner")
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

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(aclTopicOwner));

        // Force ACLs synchronization
        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        AccessControlEntry aclTopicPublic = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-public-acl-topic")
                .namespace("ns1")
                .build())
            .spec(AccessControlEntrySpec.builder()
                .resourceType(ResourceType.TOPIC)
                .resource("ns1-")
                .resourcePatternType(ResourcePatternType.PREFIXED)
                .permission(Permission.READ)
                .grantedTo("*")
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(aclTopicPublic));

        // Force ACLs synchronization
        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();

        AclBindingFilter publicFilter = new AclBindingFilter(
            ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(
                "User:*",
                null,
                AclOperation.ANY,
                AclPermissionType.ANY
            )
        );

        Collection<AclBinding> results = kafkaClient.describeAcls(publicFilter).values().get();

        AclBinding expected = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:*",
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW
            )
        );

        assertEquals(1, results.size());
        assertTrue(results.stream().findFirst().isPresent());
        assertEquals(expected, results.stream().findFirst().get());

        // DELETE the ACLs and verify
        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/acls/ns1-public-acl-topic")
                .bearerAuth(token));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/acls/ns1-acl-topic-owner")
                .bearerAuth(token));

        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(publicFilter).values().get();

        assertTrue(results.isEmpty());
    }

    @Test
    void shouldCreateAclThatDoesAlreadyExistOnBroker() throws InterruptedException, ExecutionException {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-acl-topic")
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

        AclBinding aclBinding = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW
            )
        );

        Admin kafkaClient = getAdminClient();
        kafkaClient.createAcls(Collections.singletonList(aclBinding));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(accessControlEntry));

        // Force ACLs synchronization
        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        AclBindingFilter aclBindingFilter = new AclBindingFilter(
            ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(
                "User:user1",
                null,
                AclOperation.ANY,
                AclPermissionType.ANY
            )
        );

        Collection<AclBinding> results = kafkaClient.describeAcls(aclBindingFilter).values().get();

        assertEquals(1, results.size());
        assertTrue(results.stream().findFirst().isPresent());
        assertEquals(aclBinding, results.stream().findFirst().get());

        // Remove ACL
        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/acls/ns1-acl-topic")
                .bearerAuth(token));

        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(aclBindingFilter).values().get();

        assertTrue(results.isEmpty());
    }

    @Test
    void shouldCreateConnectAclWithOwnerPermission() throws InterruptedException, ExecutionException {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-acl-connect")
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

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(accessControlEntry));

        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();

        AclBindingFilter user1Filter = new AclBindingFilter(
            ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(
                "User:user1",
                null,
                AclOperation.ANY,
                AclPermissionType.ANY
            )
        );

        Collection<AclBinding> results = kafkaClient.describeAcls(user1Filter).values().get();

        AclBinding expected = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.GROUP,
                "connect-ns1-",
                PatternType.PREFIXED),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW
            )
        );

        assertEquals(1, results.size());
        assertTrue(results.stream().findFirst().isPresent());
        assertEquals(expected, results.stream().findFirst().get());

        // DELETE the ACL and verify
        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/acls/ns1-acl-connect")
                .bearerAuth(token));

        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(user1Filter).values().get();

        assertTrue(results.isEmpty());
    }

    @Test
    void shouldCreateKafkaStreamsAcl() throws InterruptedException, ExecutionException {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
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
            .metadata(Metadata.builder()
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

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(accessControlEntry));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(aclGroup));

        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();

        AclBindingFilter aclBindingFilter = new AclBindingFilter(
            ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(
                "User:user1",
                null,
                AclOperation.ANY,
                AclPermissionType.ANY
            )
        );

        Collection<AclBinding> results = kafkaClient.describeAcls(aclBindingFilter).values().get();

        AclBinding aclBindingTopicRead = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW
            )
        );

        AclBinding aclBindingTopicWrite = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-",
                PatternType.PREFIXED),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.WRITE,
                AclPermissionType.ALLOW
            )
        );

        AclBinding aclBindingGroupRead = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.GROUP,
                "ns1-",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW
            )
        );

        AclBinding aclBindingTopicDescribeConfigs = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.DESCRIBE_CONFIGS,
                AclPermissionType.ALLOW
            )
        );

        assertEquals(4, results.size());
        assertTrue(results.containsAll(List.of(
            aclBindingTopicRead,
            aclBindingTopicWrite,
            aclBindingGroupRead,
            aclBindingTopicDescribeConfigs)
        ));

        KafkaStream kafkaStream = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("ns1-stream1")
                .namespace("ns1")
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/streams")
                .bearerAuth(token)
                .body(kafkaStream));

        // Force ACLs synchronization
        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(aclBindingFilter).values().get();

        AclBinding aclBindingTopicCreateForKafkaStreams = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-stream1",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.CREATE,
                AclPermissionType.ALLOW
            )
        );

        AclBinding aclBindingTopicDeleteForKafkaStreams = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-stream1",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.DELETE,
                AclPermissionType.ALLOW
            )
        );

        AclBinding aclBindingTransactionalForKafkaStreams = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                "ns1-stream1",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.WRITE,
                AclPermissionType.ALLOW
            )
        );

        assertEquals(7, results.size());
        assertTrue(results.containsAll(List.of(
            aclBindingTopicRead,
            aclBindingTopicWrite,
            aclBindingGroupRead,
            aclBindingTopicDescribeConfigs,
            aclBindingTopicCreateForKafkaStreams,
            aclBindingTopicDeleteForKafkaStreams,
            aclBindingTransactionalForKafkaStreams)
        ));

        // DELETE the Stream & ACL and verify
        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/streams/ns1-stream1")
                .bearerAuth(token));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/acls/ns1-acl-topic")
                .bearerAuth(token));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/acls/ns1-acl-group")
                .bearerAuth(token));

        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(aclBindingFilter).values().get();

        assertTrue(results.isEmpty());
    }

    @Test
    void shouldCreateTransactionalIdAclWithOwnerPermission() throws InterruptedException, ExecutionException {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-acl-transactional-id")
                .namespace("ns1")
                .build())
            .spec(AccessControlEntrySpec.builder()
                .resourceType(ResourceType.TRANSACTIONAL_ID)
                .resource("ns1-")
                .resourcePatternType(ResourcePatternType.PREFIXED)
                .permission(Permission.OWNER)
                .grantedTo("ns1")
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(accessControlEntry));

        // Force ACL Sync
        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();

        AclBindingFilter aclBindingFilter = new AclBindingFilter(
            ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(
                "User:user1",
                null,
                AclOperation.ANY,
                AclPermissionType.ANY
            )
        );

        Collection<AclBinding> results = kafkaClient.describeAcls(aclBindingFilter).values().get();

        AclBinding expected = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                "ns1-",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.WRITE,
                AclPermissionType.ALLOW
            )
        );

        AclBinding expected2 = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                "ns1-",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.DESCRIBE,
                AclPermissionType.ALLOW
            )
        );

        assertEquals(2, results.size());
        assertTrue(results.containsAll(List.of(expected, expected2)));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/acls/ns1-acl-transactional-id")
                .bearerAuth(token));

        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        results = kafkaClient.describeAcls(aclBindingFilter).values().get();

        assertTrue(results.isEmpty());
    }

    @Test
    void shouldGrantAclToAnotherNamespace() throws ExecutionException, InterruptedException {
        // Ownership of first namespace
        AccessControlEntry accessControlEntryNamespace1 = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-acl-topic-owner")
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

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(accessControlEntryNamespace1));

        // Force ACLs synchronization
        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        // Create second namespace
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns2")
                .cluster("test-cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("user2")
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(namespace));

        // Grant ACL from namespace 1 to namespace 2 on topic prefixed by ns1-
        AccessControlEntry grantedAclOnPrefix = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-granted-acl-on-prefix")
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

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(grantedAclOnPrefix));

        accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);

        Admin kafkaClient = getAdminClient();

        AclBindingFilter namespace2AclBindingFilter = new AclBindingFilter(
            ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(
                "User:user2",
                null,
                AclOperation.ANY,
                AclPermissionType.ANY
            )
        );

        // Verify namespace 2 has READ permission on topic prefixed by ns1-
        Collection<AclBinding> results = kafkaClient.describeAcls(namespace2AclBindingFilter).values().get();

        AclBinding expectedNamespace2TopicReadAcl = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user2",
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW
            )
        );

        assertEquals(1, results.size());
        assertTrue(results.contains(expectedNamespace2TopicReadAcl));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/acls/ns1-granted-acl-on-prefix")
                .bearerAuth(token));

        results = kafkaClient.describeAcls(namespace2AclBindingFilter).values().get();
        assertTrue(results.isEmpty());

        // Verify namespace 1 is still owner of the topic
        AclBindingFilter namespace1AclBindingFilter = new AclBindingFilter(
            ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(
                "User:user1",
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW
            )
        );

        results = kafkaClient.describeAcls(namespace1AclBindingFilter).values().get();

        AclBinding expectedNamespace1TopicReadAcl = new AclBinding(
            new ResourcePattern(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                "ns1-",
                PatternType.PREFIXED
            ),
            new org.apache.kafka.common.acl.AccessControlEntry(
                "User:user1",
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW
            )
        );

        assertEquals(1, results.size());
        assertTrue(results.contains(expectedNamespace1TopicReadAcl));
    }
}
