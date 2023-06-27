package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.integration.TopicTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.models.schema.SchemaCompatibilityState;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaResponse;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class SchemaTest extends AbstractIntegrationSchemaRegistryTest {
    @Inject
    @Client("/")
    HttpClient client;

    HttpClient schemaClient;

    private String token;

    /**
     * Init all integration tests
     */
    @BeforeAll
    void init() throws MalformedURLException {
        schemaClient = HttpClient.create(new URL(schemaRegistryContainer.getUrl()));

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
        HttpResponse<BearerAccessRefreshToken> response = client.toBlocking().exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();

        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(namespace));
        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings").bearerAuth(token).body(roleBinding));
        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls").bearerAuth(token).body(aclSchema));
    }

    /**
     * Test the schema update with a compatible v2 schema
     * - Register the schema v1
     * - Update the compatibility to forward
     * - Register the compatible schema v2
     * - Assert success
     */
    @Test
    void registerSchemaCompatibility() {
        // Register schema, first name is optional
        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject0-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        var createResponse = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schema), Schema.class);

        Assertions.assertEquals("created", createResponse.header("X-Ns4kafka-Result"));

        SchemaResponse actual = schemaClient.toBlocking()
                .retrieve(HttpRequest.GET("/subjects/ns1-subject0-value/versions/latest"),
                        SchemaResponse.class);

        Assertions.assertNotNull(actual.id());
        Assertions.assertEquals(1, actual.version());
        Assertions.assertEquals("ns1-subject0-value", actual.subject());

        // Set compat to forward
        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas/ns1-subject0-value/config")
                        .bearerAuth(token)
                        .body(Map.of("compatibility", Schema.Compatibility.FORWARD)), SchemaCompatibilityState.class);

        SchemaCompatibilityResponse updatedConfig = schemaClient.toBlocking()
                .retrieve(HttpRequest.GET("/config/ns1-subject0-value"),
                        SchemaCompatibilityResponse.class);

        Assertions.assertEquals(Schema.Compatibility.FORWARD, updatedConfig.compatibilityLevel());

        // Register compatible schema v2, removing optional "first name" field
        Schema incompatibleSchema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject0-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        var createV2Response = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(incompatibleSchema), Schema.class);

        Assertions.assertEquals("changed", createV2Response.header("X-Ns4kafka-Result"));

        SchemaResponse actualV2 = schemaClient.toBlocking()
                .retrieve(HttpRequest.GET("/subjects/ns1-subject0-value/versions/latest"),
                        SchemaResponse.class);

        Assertions.assertNotNull(actualV2.id());
        Assertions.assertEquals(2, actualV2.version());
        Assertions.assertEquals("ns1-subject0-value", actualV2.subject());
    }

    /**
     * Test the schema update with an incompatible v2 schema
     * - Register the schema v1
     * - Update the compatibility to forward
     * - Register the incompatible schema v2
     * - Assert errors
     */
    @Test
    void registerSchemaIncompatibility() {
        // Register schema, first name is non optional
        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject1-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"string\"],\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        var createResponse = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schema), Schema.class);

        Assertions.assertEquals("created", createResponse.header("X-Ns4kafka-Result"));

        SchemaResponse actual = schemaClient.toBlocking()
                .retrieve(HttpRequest.GET("/subjects/ns1-subject1-value/versions/latest"),
                        SchemaResponse.class);

        Assertions.assertNotNull(actual.id());
        Assertions.assertEquals(1, actual.version());
        Assertions.assertEquals("ns1-subject1-value", actual.subject());

        // Set compat to forward
        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas/ns1-subject1-value/config")
                        .bearerAuth(token)
                        .body(Map.of("compatibility", Schema.Compatibility.FORWARD)), SchemaCompatibilityState.class);

        SchemaCompatibilityResponse updatedConfig = schemaClient.toBlocking()
                .retrieve(HttpRequest.GET("/config/ns1-subject1-value"),
                        SchemaCompatibilityResponse.class);

        Assertions.assertEquals(Schema.Compatibility.FORWARD, updatedConfig.compatibilityLevel());

        // Register incompatible schema v2, removing non optional "first name" field
        Schema incompatibleSchema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject1-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        HttpClientResponseException incompatibleActual = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(incompatibleSchema)));

        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, incompatibleActual.getStatus());
        Assertions.assertEquals("Invalid Schema ns1-subject1-value", incompatibleActual.getMessage());
    }

    /**
     * Schema creation with references
     */
    @Test
    void registerSchemaWithReferences() {
        Schema schemaHeader = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-header-subject-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"HeaderAvro\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"ID of the header\"}]}")
                        .build())
                .build();

        Schema schemaPersonWithoutRefs = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-person-subject-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"header\",\"type\":[\"null\",\"com.michelin.kafka.producer.showcase.avro.HeaderAvro\"],\"default\":null,\"doc\":\"Header of the persone\"},{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        Schema schemaPersonWithRefs = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-person-subject-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"header\",\"type\":[\"null\",\"com.michelin.kafka.producer.showcase.avro.HeaderAvro\"],\"default\":null,\"doc\":\"Header of the persone\"},{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .references(List.of(Schema.SchemaSpec.Reference.builder()
                                .name("com.michelin.kafka.producer.showcase.avro.HeaderAvro")
                                .subject("ns1-header-subject-value")
                                .version(1)
                                .build()))
                        .build())
                .build();

        // Header created
        var headerCreateResponse = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schemaHeader), Schema.class);

        Assertions.assertEquals("created", headerCreateResponse.header("X-Ns4kafka-Result"));

        SchemaResponse actualHeader = schemaClient.toBlocking().retrieve(HttpRequest.GET("/subjects/ns1-header-subject-value/versions/latest"),
                SchemaResponse.class);

        Assertions.assertNotNull(actualHeader.id());
        Assertions.assertEquals(1, actualHeader.version());
        Assertions.assertEquals("ns1-header-subject-value", actualHeader.subject());

        // Person without refs not created
        HttpClientResponseException createException = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schemaPersonWithoutRefs)));

        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, createException.getStatus());
        Assertions.assertEquals("Invalid Schema ns1-person-subject-value", createException.getMessage());

        // Person with refs created
        var personCreateResponse = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schemaPersonWithRefs), Schema.class);

        Assertions.assertEquals("created", personCreateResponse.header("X-Ns4kafka-Result"));

        SchemaResponse actualPerson = schemaClient.toBlocking().retrieve(HttpRequest.GET("/subjects/ns1-person-subject-value/versions/latest"),
                SchemaResponse.class);

        Assertions.assertNotNull(actualPerson.id());
        Assertions.assertEquals(1, actualPerson.version());
        Assertions.assertEquals("ns1-person-subject-value", actualPerson.subject());
    }

    /**
     * Schema creation with prefix that does not respect ACLs
     */
    @Test
    void registerSchemaWrongPrefix() {
        Schema wrongSchema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("wrongprefix-subject")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                        .build())
                .build();

        HttpClientResponseException createException = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                                .bearerAuth(token)
                                .body(wrongSchema)));

        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, createException.getStatus());
        Assertions.assertEquals("Invalid Schema wrongprefix-subject", createException.getMessage());

        HttpClientResponseException getException = Assertions.assertThrows(HttpClientResponseException.class,
                () -> schemaClient.toBlocking()
                        .retrieve(HttpRequest.GET("/subjects/wrongprefix-subject/versions/latest"),
                                SchemaResponse.class));

        Assertions.assertEquals(HttpStatus.NOT_FOUND, getException.getStatus());
    }

    /**
     * Schema creation and deletion
     */
    @Test
    void registerSchema() {
        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1-subject2-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                        .build())
                .build();

        var createResponse = client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"/api/namespaces/ns1/schemas")
                        .bearerAuth(token)
                        .body(schema), Schema.class);

        Assertions.assertEquals("created", createResponse.header("X-Ns4kafka-Result"));

        var deleteResponse = client.toBlocking().exchange(HttpRequest.create(HttpMethod.DELETE,"/api/namespaces/ns1/schemas/ns1-subject2-value")
                        .bearerAuth(token), Schema.class);

        Assertions.assertEquals(HttpStatus.NO_CONTENT, deleteResponse.getStatus());

        HttpClientResponseException getException = Assertions.assertThrows(HttpClientResponseException.class,
                () -> schemaClient.toBlocking()
                        .retrieve(HttpRequest.GET("/subjects/ns1-subject2-value/versions/latest"),
                                SchemaResponse.class));

        Assertions.assertEquals(HttpStatus.NOT_FOUND, getException.getStatus());
    }
}
