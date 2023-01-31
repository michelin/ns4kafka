package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.ResourceValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class ConnectValidatorTest {
    @Test
    void testEquals(){
        ConnectValidator original = ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter", new ResourceValidator.NonEmptyString(),
                        "value.converter", new ResourceValidator.NonEmptyString(),
                        "connector.class", new ResourceValidator.ValidString(
                                List.of("io.confluent.connect.jdbc.JdbcSourceConnector",
                                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                                        "com.splunk.kafka.connect.SplunkSinkConnector",
                                        "org.apache.kafka.connect.file.FileStreamSinkConnector"),
                                false)))
                .sourceValidationConstraints(Map.of(
                        "producer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .sinkValidationConstraints(Map.of(
                        "consumer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .classValidationConstraints(Map.of(
                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                        Map.of("db.timezone", new ResourceValidator.NonEmptyString())))
                .build();

        ConnectValidator same = ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter", new ResourceValidator.NonEmptyString(),
                        "value.converter", new ResourceValidator.NonEmptyString(),
                        "connector.class", new ResourceValidator.ValidString(
                                List.of("io.confluent.connect.jdbc.JdbcSourceConnector",
                                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                                        "com.splunk.kafka.connect.SplunkSinkConnector",
                                        "org.apache.kafka.connect.file.FileStreamSinkConnector"),
                                false)))
                .sourceValidationConstraints(Map.of(
                        "producer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .sinkValidationConstraints(Map.of(
                        "consumer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .classValidationConstraints(Map.of(
                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                        Map.of(
                                "db.timezone", new ResourceValidator.NonEmptyString())))
                .build();

        ConnectValidator differentByGeneralRules = ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter", new ResourceValidator.NonEmptyString()))
                .sourceValidationConstraints(Map.of(
                        "producer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .sinkValidationConstraints(Map.of(
                        "consumer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .classValidationConstraints(Map.of(
                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                        Map.of(
                                "db.timezone", new ResourceValidator.NonEmptyString())))
                .build();

        ConnectValidator differentBySourceRules = ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter", new ResourceValidator.NonEmptyString(),
                        "value.converter", new ResourceValidator.NonEmptyString(),
                        "connector.class", new ResourceValidator.ValidString(
                                List.of("io.confluent.connect.jdbc.JdbcSourceConnector",
                                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                                        "com.splunk.kafka.connect.SplunkSinkConnector",
                                        "org.apache.kafka.connect.file.FileStreamSinkConnector"),
                                false)))
                .sourceValidationConstraints(Map.of())
                .sinkValidationConstraints(Map.of(
                        "consumer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .classValidationConstraints(Map.of(
                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                        Map.of(
                                "db.timezone", new ResourceValidator.NonEmptyString())))
                .build();

        ConnectValidator differentBySinkRules = ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter", new ResourceValidator.NonEmptyString(),
                        "value.converter", new ResourceValidator.NonEmptyString(),
                        "connector.class", new ResourceValidator.ValidString(
                                List.of("io.confluent.connect.jdbc.JdbcSourceConnector",
                                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                                        "com.splunk.kafka.connect.SplunkSinkConnector",
                                        "org.apache.kafka.connect.file.FileStreamSinkConnector"),
                                false)))
                .sourceValidationConstraints(Map.of(
                        "producer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .sinkValidationConstraints(Map.of())
                .classValidationConstraints(Map.of(
                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                        Map.of(
                                "db.timezone", new ResourceValidator.NonEmptyString())))
                .build();

        ConnectValidator differentByClassRules = ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter", new ResourceValidator.NonEmptyString(),
                        "value.converter", new ResourceValidator.NonEmptyString(),
                        "connector.class", new ResourceValidator.ValidString(
                                List.of("io.confluent.connect.jdbc.JdbcSourceConnector",
                                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                                        "com.splunk.kafka.connect.SplunkSinkConnector",
                                        "org.apache.kafka.connect.file.FileStreamSinkConnector"),
                                false)))
                .sourceValidationConstraints(Map.of(
                        "producer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .sinkValidationConstraints(Map.of(
                        "consumer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .classValidationConstraints(Map.of(
                        "io.confluent.connect.jdbc.JdbcSinkConnector_oops", // <<<<< here oops
                        Map.of(
                                "db.timezone", new ResourceValidator.NonEmptyString())))
                .build();

        Assertions.assertEquals(original, same);
        Assertions.assertNotEquals(original, differentByGeneralRules);
        Assertions.assertNotEquals(original, differentBySourceRules);
        Assertions.assertNotEquals(original, differentBySinkRules);
        Assertions.assertNotEquals(original, differentByClassRules);
    }

    @Test
    void shouldValidateWithNoClassValidationConstraint() {
        ConnectValidator noValidationConstraintValidator = ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter", new ResourceValidator.NonEmptyString(),
                        "value.converter", new ResourceValidator.NonEmptyString(),
                        "connector.class", new ResourceValidator.ValidString(
                                List.of("io.confluent.connect.jdbc.JdbcSourceConnector",
                                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                                        "com.splunk.kafka.connect.SplunkSinkConnector",
                                        "org.apache.kafka.connect.file.FileStreamSinkConnector"),
                                false)))
                .sourceValidationConstraints(Map.of(
                        "producer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .sinkValidationConstraints(Map.of(
                        "consumer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .build();

        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder()
                        .name("connect2")
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("cluster1")
                        .config(Map.of(
                                "connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector",
                                "key.converter", "test",
                                "value.converter", "test",
                                "consumer.override.sasl.jaas.config", "test"))
                        .build())
                .build();

        List<String> actual = noValidationConstraintValidator.validate(connector, "sink");
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void shouldValidateWithNoSinkValidationConstraint() {
        ConnectValidator noValidationConstraintValidator = ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter", new ResourceValidator.NonEmptyString(),
                        "value.converter", new ResourceValidator.NonEmptyString(),
                        "connector.class", new ResourceValidator.ValidString(
                                List.of("io.confluent.connect.jdbc.JdbcSourceConnector",
                                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                                        "com.splunk.kafka.connect.SplunkSinkConnector",
                                        "org.apache.kafka.connect.file.FileStreamSinkConnector"),
                                false)))
                .sourceValidationConstraints(Map.of(
                        "producer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .build();

        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder()
                        .name("connect2")
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("cluster1")
                        .config(Map.of(
                                "connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector",
                                "key.converter", "test",
                                "value.converter", "test"))
                        .build())
                .build();

        List<String> actual = noValidationConstraintValidator.validate(connector, "sink");
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void shouldValidateWithNoValidationConstraint() {
        ConnectValidator noValidationConstraintValidator = ConnectValidator.builder()
                .build();

        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder()
                        .name("connect2")
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("cluster1")
                        .config(Map.of(
                                "connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector",
                                "key.converter", "test",
                                "value.converter", "test"))
                        .build())
                .build();

        List<String> actual = noValidationConstraintValidator.validate(connector, "sink");
        Assertions.assertTrue(actual.isEmpty());
    }
}
