package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStateInfo;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaResponse;
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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

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
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
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
    void createAndGetSchema() throws MalformedURLException {
        RxHttpClient schemaCli = RxHttpClient.create(new URL(schemaRegistryContainer.getUrl()));
        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        var createResponse = client
                .exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schema), Schema.class).blockingFirst();

        Assertions.assertEquals("created", createResponse.header("X-Ns4kafka-Result"));

        SchemaResponse actual = schemaCli.retrieve(HttpRequest.GET("/subjects/ns1-subject-value/versions/latest"),
                SchemaResponse.class).blockingFirst();

        Assertions.assertNotNull(actual.id());
        Assertions.assertEquals(1, actual.version());
        Assertions.assertEquals("ns1-subject-value", actual.subject());
    }

    /**
     * Schema creation with prefix that does not respect ACLs
     */
    @Test
    void createAndGetSchemaWrongPrefix() throws MalformedURLException {
        RxHttpClient schemaCli = RxHttpClient.create(new URL(schemaRegistryContainer.getUrl()));
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
                () -> schemaCli.retrieve(HttpRequest.GET("/subjects/wrongprefix-subject/versions/latest"),
                        SchemaResponse.class).blockingFirst());

        Assertions.assertEquals(HttpStatus.NOT_FOUND, getException.getStatus());
    }

    /**
     * Compatibility update
     *
     * Create a schema with no compatibility (global compatibility), set the compatibility to
     * NONE, then reset the compatibility to the global one
     */
    @Test
    void updateCompatibility() throws MalformedURLException {
        RxHttpClient schemaCli = RxHttpClient.create(new URL(schemaRegistryContainer.getUrl()));
        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject3-value")
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

        SchemaResponse actual = schemaCli.retrieve(HttpRequest.GET("/subjects/ns1-subject3-value/versions/latest"),
                SchemaResponse.class).blockingFirst();

        Assertions.assertNotNull(actual.id());
        Assertions.assertEquals(1, actual.version());
        Assertions.assertEquals("ns1-subject3-value", actual.subject());

        HttpClientResponseException exception = Assertions.assertThrows(HttpClientResponseException.class, () ->
                schemaCli.retrieve(HttpRequest.GET("/config/ns1-subject3-value"),
                        SchemaCompatibilityResponse.class).blockingFirst());

        Assertions.assertEquals(HttpStatus.NOT_FOUND, exception.getStatus());

        client
                .exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas/ns1-subject3-value/config")
                        .bearerAuth(token)
                        .body(Map.of("compatibility", Schema.Compatibility.NONE)), SchemaCompatibilityState.class).blockingFirst();

        SchemaCompatibilityResponse updatedConfig = schemaCli.retrieve(HttpRequest.GET("/config/ns1-subject3-value"),
                SchemaCompatibilityResponse.class).blockingFirst();

        Assertions.assertEquals(Schema.Compatibility.NONE, updatedConfig.compatibilityLevel());

         client
                .exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas/ns1-subject3-value/config")
                        .bearerAuth(token)
                        .body(Map.of("compatibility", Schema.Compatibility.DEFAULT)), SchemaCompatibilityState.class).blockingFirst();

        HttpClientResponseException resetConfigException = Assertions.assertThrows(HttpClientResponseException.class, () ->
                schemaCli.retrieve(HttpRequest.GET("/config/ns1-subject3-value"),
                        SchemaCompatibilityResponse.class).blockingFirst());

        Assertions.assertEquals(HttpStatus.NOT_FOUND, resetConfigException.getStatus());
    }

    /**
     * Schema creation and deletion
     */
    @Test
    void createAndDeleteSchema() throws MalformedURLException {
        RxHttpClient schemaCli = RxHttpClient.create(new URL(schemaRegistryContainer.getUrl()));
        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject4-value")
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

        var deleteResponse = client
                .exchange(HttpRequest.create(HttpMethod.DELETE,"/api/namespaces/ns1/schemas/ns1-subject4-value")
                        .bearerAuth(token), Schema.class).blockingFirst();

        Assertions.assertEquals(HttpStatus.NO_CONTENT, deleteResponse.getStatus());

        HttpClientResponseException getException = Assertions.assertThrows(HttpClientResponseException.class,
                () -> schemaCli.retrieve(HttpRequest.GET("/subjects/ns1-subject4-value/versions/latest"),
                        SchemaResponse.class).blockingFirst());

        Assertions.assertEquals(HttpStatus.NOT_FOUND, getException.getStatus());
    }
}
