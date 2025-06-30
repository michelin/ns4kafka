/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.integration;

import static com.michelin.ns4kafka.util.config.TopicConfig.*;
import static org.junit.jupiter.api.Assertions.*;

import com.michelin.ns4kafka.integration.TopicIntegrationTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.integration.container.SchemaRegistryIntegrationTest;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SubjectNameStrategy;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaResponse;
import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.BlockingHttpClient;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
class SchemaSubjectNameStrategyIntegrationTest extends SchemaRegistryIntegrationTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    @Client("/")
    private HttpClient ns4KafkaClient;

    private HttpClient schemaRegistryClient;

    private String token;

    private static TopicValidator makeDefaultTopicValidatorWithStrategies() {
        return TopicValidator.builder()
                .validationConstraints(Map.of(
                        REPLICATION_FACTOR,
                        ResourceValidator.Range.between(3, 3),
                        PARTITIONS,
                        ResourceValidator.Range.between(3, 6),
                        "cleanup.policy",
                        ResourceValidator.ValidList.in("delete", "compact"),
                        "min.insync.replicas",
                        ResourceValidator.Range.between(2, 2),
                        "retention.ms",
                        ResourceValidator.Range.between(60000, 604800000),
                        "retention.bytes",
                        ResourceValidator.Range.optionalBetween(-1, 104857600),
                        "preallocate",
                        ResourceValidator.ValidString.optionalIn("true", "false"),
                        VALUE_SUBJECT_NAME_STRATEGY,
                        ResourceValidator.ValidString.optionalIn(
                                SubjectNameStrategy.TOPIC_RECORD_NAME.toString(),
                                SubjectNameStrategy.RECORD_NAME.toString())))
                .build();
    }

    @BeforeAll
    void init() {
        // Create HTTP client as bean to load client configuration from application.yml
        schemaRegistryClient = applicationContext.createBean(HttpClient.class, getSchemaRegistryUrl());

        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder().name("ns1").cluster("test-cluster").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .topicValidator(makeDefaultTopicValidatorWithStrategies())
                        .build())
                .build();

        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(Metadata.builder().name("ns1-rb").namespace("ns1").build())
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

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<BearerAccessRefreshToken> response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        token = response.getBody().get().getAccessToken();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                        .bearerAuth(token)
                        .body(namespace));

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings")
                        .bearerAuth(token)
                        .body(roleBinding));

        AccessControlEntry aclSchema = AccessControlEntry.builder()
                .metadata(Metadata.builder().name("ns1-acl").namespace("ns1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                        .bearerAuth(token)
                        .body(aclSchema));
    }

    @Test
    void shouldRegisterSchemaWithReferencesAndTopicRecordNameStrategy() {
        String subject = "ns1-header-subject-com.michelin.kafka.producer.showcase.avro.HeaderAvro";
        Schema schemaHeader = Schema.builder()
                .metadata(Metadata.builder().name(subject).build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                                + "\"name\":\"HeaderAvro\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],"
                                + "\"default\":null,\"doc\":\"ID of the header\"}]}")
                        .build())
                .build();

        // Header created
        var headerCreateResponse = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/schemas")
                                .bearerAuth(token)
                                .body(schemaHeader),
                        Schema.class);

        assertEquals("created", headerCreateResponse.header("X-Ns4kafka-Result"));

        SchemaResponse actualHeader = schemaRegistryClient
                .toBlocking()
                .retrieve(HttpRequest.GET("/subjects/" + subject + "/versions/latest"), SchemaResponse.class);

        assertNotNull(actualHeader.id());
        assertEquals(1, actualHeader.version());
        assertEquals(subject, actualHeader.subject());

        // Person without refs not created
        String personSubject = "ns1-person-subject-com.michelin.kafka.producer.showcase.avro.PersonAvro";
        Schema schemaPersonWithoutRefs = Schema.builder()
                .metadata(Metadata.builder().name(personSubject).build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                                + "\"name\":\"PersonAvro\","
                                + "\"fields\":[{\"name\":\"header\",\"type\":[\"null\","
                                + "\"com.michelin.kafka.producer.showcase.avro.HeaderAvro\"],"
                                + "\"default\":null,\"doc\":\"Header of the person\"},{\"name\":\"firstName\","
                                + "\"type\":[\"null\",\"string\"],"
                                + "\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\","
                                + "\"type\":[\"null\",\"string\"],"
                                + "\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\","
                                + "\"type\":[\"null\","
                                + "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null"
                                + "\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        HttpRequest<?> request = HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/schemas")
                .bearerAuth(token)
                .body(schemaPersonWithoutRefs);

        BlockingHttpClient blockingClient = ns4KafkaClient.toBlocking();

        HttpClientResponseException createException =
                assertThrows(HttpClientResponseException.class, () -> blockingClient.exchange(request));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, createException.getStatus());
        assertEquals("Resource validation failed", createException.getMessage());

        // Person with refs created
        Schema schemaPersonWithRefs = Schema.builder()
                .metadata(Metadata.builder().name(personSubject).build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\","
                                + "\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"header\",\"type\":[\"null\","
                                + "\"com.michelin.kafka.producer.showcase.avro.HeaderAvro\"],"
                                + "\"default\":null,\"doc\":\"Header of the person\"},{\"name\":\"firstName\","
                                + "\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},"
                                + "{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":"
                                + "\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\","
                                + "\"logicalType\":\"timestamp-millis\"}],\"default\":null,"
                                + "\"doc\":\"Date of birth of the person\"}]}")
                        .references(List.of(Schema.SchemaSpec.Reference.builder()
                                .name("com.michelin.kafka.producer.showcase.avro.HeaderAvro")
                                .subject(subject)
                                .version(1)
                                .build()))
                        .build())
                .build();

        var personCreateResponse = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/schemas")
                                .bearerAuth(token)
                                .body(schemaPersonWithRefs),
                        Schema.class);

        assertEquals("created", personCreateResponse.header("X-Ns4kafka-Result"));

        SchemaResponse actualPerson = schemaRegistryClient
                .toBlocking()
                .retrieve(HttpRequest.GET("/subjects/" + personSubject + "/versions/latest"), SchemaResponse.class);

        assertNotNull(actualPerson.id());
        assertEquals(1, actualPerson.version());
        assertEquals(personSubject, actualPerson.subject());
    }

    @Test
    void shouldRegisterSchema() {
        String personSubject = "ns1-person-subject-com.michelin.kafka.producer.showcase.avro.PersonAvro";
        Schema schema = Schema.builder()
                .metadata(Metadata.builder().name(personSubject).build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                                + "\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],"
                                + "\"default\":null,\"doc\":\"First name of the person\"},"
                                + "{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                                + "\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\","
                                + "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,"
                                + "\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();

        // Apply schema
        var createResponse = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/schemas")
                                .bearerAuth(token)
                                .body(schema),
                        Schema.class);

        assertEquals("created", createResponse.header("X-Ns4kafka-Result"));

        // Get all schemas
        var getResponse = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.GET, "/api/namespaces/ns1/schemas")
                                .bearerAuth(token),
                        Argument.listOf(Schema.class));

        assertTrue(getResponse.getBody().isPresent());
        assertTrue(getResponse.getBody().get().stream()
                .anyMatch(
                        schemaResponse -> schemaResponse.getMetadata().getName().equals(personSubject)));

        // Delete schema
        var deleteResponse = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.DELETE, "/api/namespaces/ns1/schemas/" + personSubject)
                                .bearerAuth(token),
                        Schema.class);

        assertEquals(HttpStatus.NO_CONTENT, deleteResponse.getStatus());

        // Get all schemas
        var getResponseEmpty = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.GET, "/api/namespaces/ns1/schemas")
                                .bearerAuth(token),
                        Argument.listOf(Schema.class));

        assertTrue(getResponseEmpty.getBody().isPresent());
        assertTrue(getResponseEmpty.getBody().get().stream()
                .noneMatch(
                        schemaResponse -> schemaResponse.getMetadata().getName().equals(personSubject)));

        HttpRequest<?> request = HttpRequest.GET("/subjects/" + personSubject + "/versions/latest");

        BlockingHttpClient blockingSchemaRegistryClient = schemaRegistryClient.toBlocking();

        HttpClientResponseException getException = assertThrows(
                HttpClientResponseException.class,
                () -> blockingSchemaRegistryClient.retrieve(request, SchemaResponse.class));

        assertEquals(HttpStatus.NOT_FOUND, getException.getStatus());
    }
}
