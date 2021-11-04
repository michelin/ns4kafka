package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.models.*;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class SchemaTest extends AbstractIntegrationSchemaRegistryTest {
    /**
     * HTTP client
     */
    @Inject
    @Client("/")
    RxHttpClient client;

    /**
     * Authentication token
     */
    private String token;

    /**
     * Init all integration tests
     */
    @BeforeAll
    void init() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1")
                        .cluster("test-cluster")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .build())
                .build();

        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-rb")
                        .namespace("ns1")
                        .build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        AccessControlEntry aclSchema = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-acl")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.SCHEMA)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicTest.BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), TopicTest.BearerAccessRefreshToken.class).blockingFirst();

        token = response.getBody().get().getAccessToken();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(namespace)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings").bearerAuth(token).body(roleBinding)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls").bearerAuth(token).body(aclSchema)).blockingFirst();
    }

    /**
     * Schema creation
     */
    @Test
    void createAndGetSchema() {
        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                        .build())
                .build();

        var createResponse = client
                .exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schema), Schema.class).blockingFirst();

        Assertions.assertEquals("created", createResponse.header("X-Ns4kafka-Result"));
        Assertions.assertTrue(createResponse.getBody().isPresent());
        Assertions.assertNotNull(createResponse.getBody().get().getSpec().getId());
        Assertions.assertEquals(1, createResponse.getBody().get().getSpec().getVersion());

        var getResponse = client
                .exchange(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns1/schemas/ns1-subject")
                        .bearerAuth(token), Schema.class).blockingFirst();

        Assertions.assertTrue(getResponse.getBody().isPresent());
        Assertions.assertNotNull(getResponse.getBody().get().getSpec().getId());
        Assertions.assertEquals(1, getResponse.getBody().get().getSpec().getVersion());
    }

    /**
     * Schema creation with prefix that does not respect ACLs
     */
    @Test
    void createAndGetSchemaWrongPrefix() {
        Schema wrongSchema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("wrongprefix-subject")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                        .build())
                .build();

        HttpClientResponseException createException = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(wrongSchema)).blockingFirst());

        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, createException.getStatus());
        Assertions.assertEquals("Invalid Schema wrongprefix-subject", createException.getMessage());

        HttpClientResponseException getException = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns1/schemas/wrongprefix-subject")
                        .bearerAuth(token), Schema.class).blockingFirst());

        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, getException.getStatus());
    }

    /**
     * Compatibility update
     *
     * Create a schema with no compatibility (global compatibility), set the compatibility to
     * NONE, then reset the compatibility to the global one
     */
    @Test
    void updateCompatibility() {
        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject3")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                        .build())
                .build();

        var createResponse = client
                .exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schema), Schema.class).blockingFirst();

        Assertions.assertEquals("created", createResponse.header("X-Ns4kafka-Result"));
        Assertions.assertTrue(createResponse.getBody().isPresent());
        Assertions.assertNotNull(createResponse.getBody().get().getSpec().getId());
        Assertions.assertEquals(1, createResponse.getBody().get().getSpec().getVersion());
        Assertions.assertEquals(Schema.Compatibility.GLOBAL, createResponse.getBody().get().getSpec().getCompatibility());

        var updateCompatibilityResponse = client
                .exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas/ns1-subject3/compatibility")
                        .bearerAuth(token)
                        .body(Collections.singletonMap("compatibility", Schema.Compatibility.NONE)), Schema.class).blockingFirst();

        Assertions.assertEquals("changed", updateCompatibilityResponse.header("X-Ns4kafka-Result"));
        Assertions.assertTrue(updateCompatibilityResponse.getBody().isPresent());
        Assertions.assertNotNull(updateCompatibilityResponse.getBody().get().getSpec().getId());
        Assertions.assertEquals(1, updateCompatibilityResponse.getBody().get().getSpec().getVersion());
        Assertions.assertEquals(Schema.Compatibility.NONE, updateCompatibilityResponse.getBody().get().getSpec().getCompatibility());

        var resetCompatibilityResponse = client
                .exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas/ns1-subject3/compatibility")
                        .bearerAuth(token)
                        .body(Collections.singletonMap("compatibility", Schema.Compatibility.GLOBAL)), Schema.class).blockingFirst();

        Assertions.assertEquals("changed", resetCompatibilityResponse.header("X-Ns4kafka-Result"));
        Assertions.assertTrue(resetCompatibilityResponse.getBody().isPresent());
        Assertions.assertNotNull(resetCompatibilityResponse.getBody().get().getSpec().getId());
        Assertions.assertEquals(1, resetCompatibilityResponse.getBody().get().getSpec().getVersion());
        Assertions.assertEquals(Schema.Compatibility.GLOBAL, resetCompatibilityResponse.getBody().get().getSpec().getCompatibility());
    }

    /**
     * Schema creation and deletion
     */
    @Test
    void createAndDeleteSchema() {
        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject4")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                        .build())
                .build();

        var createResponse = client
                .exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schema), Schema.class).blockingFirst();

        Assertions.assertEquals("created", createResponse.header("X-Ns4kafka-Result"));
        Assertions.assertTrue(createResponse.getBody().isPresent());
        Assertions.assertNotNull(createResponse.getBody().get().getSpec().getId());
        Assertions.assertEquals(1, createResponse.getBody().get().getSpec().getVersion());

        var deleteResponse = client
                .exchange(HttpRequest.create(HttpMethod.DELETE,"/api/namespaces/ns1/schemas/ns1-subject4")
                        .bearerAuth(token), Schema.class).blockingFirst();

        Assertions.assertEquals(HttpStatus.NO_CONTENT, deleteResponse.getStatus());

        HttpClientResponseException getException = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.exchange(HttpRequest.create(HttpMethod.GET,"/api/namespaces/ns1/schemas/ns1-subject4")
                        .bearerAuth(token), Schema.class).blockingFirst());

        Assertions.assertEquals(HttpStatus.NOT_FOUND, getException.getStatus());
    }
}
