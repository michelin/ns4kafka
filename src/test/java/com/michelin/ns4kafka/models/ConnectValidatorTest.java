package com.michelin.ns4kafka.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.ResourceValidator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectValidatorTest {
    @Test
    void testEquals() {
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

        assertEquals(original, same);
        assertNotEquals(original, differentByGeneralRules);
        assertNotEquals(original, differentBySourceRules);

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

        assertNotEquals(original, differentBySinkRules);

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

        assertNotEquals(original, differentByClassRules);
    }

    @Test
    void shouldNotValidateConnectorWithNoName() {
        ConnectValidator validator = ConnectValidator.builder()
            .build();

        Connector connector = Connector.builder()
            .metadata(ObjectMeta.builder()
                .build())
            .build();

        List<String> actual = validator.validate(connector, "sink");
        assertEquals(1, actual.size());
        assertEquals("Invalid value null for name: Value must not be empty", actual.get(0));
    }

    @Test
    void shouldNotValidateConnectorBecauseNameLengthAndSpecialChars() {
        ConnectValidator validator = ConnectValidator.builder()
            .build();

        Connector connector = Connector.builder()
            .metadata(ObjectMeta.builder()
                .name(
                    "$thisNameIsDefinitelyToLooooooooooooooooooooooooooooooooooooooooooo"
                        + "ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                        + "ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                        + "oooooooooooooooooooooooooooooooooooooooongToBeAConnectorName$")
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

        List<String> actual = validator.validate(connector, "sink");
        assertEquals(2, actual.size());
        assertEquals(
            "Invalid value $thisNameIsDefinitelyToLoooooooooooooooo"
                + "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "ooooooooooooooongToBeAConnectorName$ for name: Value must not be longer than 249",
            actual.get(0));
        assertEquals(
            "Invalid value $thisNameIsDefinitelyToLooooooooooooooooooooooo"
                + "ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                + "oooooooooooooooooooooooooooooooooooooooooooooooooooongToBeAConnectorName$ for name: "
                + "Value must only contain ASCII alphanumerics, '.', '_' or '-'",
            actual.get(1));
    }

    @Test
    void shouldValidateWithNoClassValidationConstraint() {
        ConnectValidator validator = ConnectValidator.builder()
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

        List<String> actual = validator.validate(connector, "sink");
        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldValidateWithNoSinkValidationConstraint() {
        ConnectValidator validator = ConnectValidator.builder()
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
                    "connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "key.converter", "test",
                    "value.converter", "test"))
                .build())
            .build();

        List<String> actual = validator.validate(connector, "sink");
        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldValidateWithNoValidationConstraint() {
        ConnectValidator validator = ConnectValidator.builder()
            .build();

        Connector connector = Connector.builder()
            .metadata(ObjectMeta.builder()
                .name("connect2")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster1")
                .config(Map.of(
                    "connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "key.converter", "test",
                    "value.converter", "test"))
                .build())
            .build();

        List<String> actual = validator.validate(connector, "sink");
        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldValidateSourceConnector() {
        ConnectValidator validator = ConnectValidator.builder()
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
                    "producer.override.sasl.jaas.config", "test"))
                .build())
            .build();

        List<String> actual = validator.validate(connector, "source");
        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldNotValidateSourceConnector() {
        ConnectValidator validator = ConnectValidator.builder()
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

        List<String> actual = validator.validate(connector, "source");
        assertEquals(1, actual.size());
        assertEquals("Invalid value null for configuration producer.override.sasl.jaas.config: Value must be non-null",
            actual.get(0));
    }

    @Test
    void shouldNotValidateSinkConnector() {
        ConnectValidator validator = ConnectValidator.builder()
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

        Connector connector = Connector.builder()
            .metadata(ObjectMeta.builder()
                .name("connect2")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster1")
                .config(Map.of(
                    "connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "key.converter", "test",
                    "value.converter", "test"))
                .build())
            .build();

        List<String> actual = validator.validate(connector, "sink");
        assertEquals(2, actual.size());
        assertTrue(actual.contains(
            "Invalid value null for configuration consumer.override.sasl.jaas.config: Value must be non-null"));
        assertTrue(actual.contains("Invalid value null for configuration db.timezone: Value must be non-null"));
    }
}
