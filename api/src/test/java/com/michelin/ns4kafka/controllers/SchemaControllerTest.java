package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.SchemaService;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SchemaControllerTest {
    /**
     * The schema service
     */
    @Mock
    SchemaService schemaService;

    /**
     * The security service
     */
    @Mock
    SecurityService securityService;

    /**
     * The namespace service
     */
    @Mock
    NamespaceService namespaceService;

    /**
     * The application event publisher
     */
    @Mock
    ApplicationEventPublisher applicationEventPublisher;

    /**
     * The schema controller
     */
    @InjectMocks
    SchemaController schemaController;

    /**
     * Test the schema creation
     */
    @Test
    void create() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                        .build())
                .build();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(true);
        when(this.schemaService.findByName(namespace, "prefix.schema")).thenReturn(Optional.empty());
        when(this.securityService.username()).thenReturn(Optional.of("test-user"));
        when(this.securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        when(this.schemaService.create(schema)).thenReturn(schema);

        var response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.schema", actual.getMetadata().getName());
    }

    /**
     * Test the schema creation when the schema already exists
     */
    @Test
    void createAlreadyExisting() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                        .build())
                .build();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(true);
        when(this.schemaService.findByName(namespace, "prefix.schema")).thenReturn(Optional.of(schema));

        var response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.schema", actual.getMetadata().getName());
    }
}
