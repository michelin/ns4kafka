package com.michelin.ns4kafka.validation;

import static com.michelin.ns4kafka.utils.FormatErrorUtils.invalidNameEmpty;
import static com.michelin.ns4kafka.utils.FormatErrorUtils.invalidNameLength;
import static com.michelin.ns4kafka.utils.FormatErrorUtils.invalidNameSpecChars;
import static com.michelin.ns4kafka.utils.config.ConnectorConfig.CONNECTOR_CLASS;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.michelin.ns4kafka.models.connector.Connector;
import io.micronaut.core.util.StringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Validator for connectors.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ConnectValidator extends ResourceValidator {
    @Builder.Default
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    private Map<String, Validator> sourceValidationConstraints = new HashMap<>();

    @Builder.Default
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    private Map<String, Validator> sinkValidationConstraints = new HashMap<>();

    @Builder.Default
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    private Map<String, Map<String, Validator>> classValidationConstraints = new HashMap<>();

    /**
     * Make a default ConnectValidator.
     *
     * @return A ConnectValidator
     */
    public static ConnectValidator makeDefault() {
        return ConnectValidator.builder()
            .validationConstraints(Map.of(
                "key.converter", new ResourceValidator.NonEmptyString(),
                "value.converter", new ResourceValidator.NonEmptyString(),
                CONNECTOR_CLASS, new ResourceValidator.ValidString(
                    List.of("io.confluent.connect.jdbc.JdbcSourceConnector",
                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                        "com.splunk.kafka.connect.SplunkSinkConnector",
                        "org.apache.kafka.connect.file.FileStreamSinkConnector"),
                    false
                )
            ))
            .sourceValidationConstraints(Map.of(
                "producer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()
            ))
            .sinkValidationConstraints(Map.of(
                "consumer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()
            ))
            .classValidationConstraints(Map.of(
                "io.confluent.connect.jdbc.JdbcSinkConnector",
                Map.of(
                    "db.timezone", new ResourceValidator.NonEmptyString()
                )
            ))
            .build();
    }

    /**
     * Validate a given connector.
     *
     * @param connector     The connector
     * @param connectorType The connector type
     * @return A list of validation errors
     */
    public List<String> validate(Connector connector, String connectorType) {
        List<String> validationErrors = new ArrayList<>();

        if (!StringUtils.hasText(connector.getMetadata().getName())) {
            return List.of(invalidNameEmpty());
        }

        if (connector.getMetadata().getName().length() > 249) {
            validationErrors.add(invalidNameLength(connector.getMetadata().getName()));
        }

        if (!connector.getMetadata().getName().matches("[a-zA-Z0-9._-]+")) {
            validationErrors.add(invalidNameSpecChars(connector.getMetadata().getName()));
        }

        validationConstraints.forEach((key, value) -> {
            try {
                value.ensureValid(key, connector.getSpec().getConfig().get(key));
            } catch (FieldValidationException e) {
                validationErrors.add(e.getMessage());
            }
        });

        if (connectorType.equals("sink")) {
            sinkValidationConstraints.forEach((key, value) -> {
                try {
                    value.ensureValid(key, connector.getSpec().getConfig().get(key));
                } catch (FieldValidationException e) {
                    validationErrors.add(e.getMessage());
                }
            });
        }

        if (connectorType.equals("source")) {
            sourceValidationConstraints.forEach((key, value) -> {
                try {
                    value.ensureValid(key, connector.getSpec().getConfig().get(key));
                } catch (FieldValidationException e) {
                    validationErrors.add(e.getMessage());
                }
            });
        }

        if (classValidationConstraints.containsKey(connector.getSpec().getConfig().get(CONNECTOR_CLASS))) {
            classValidationConstraints.get(connector.getSpec().getConfig().get(CONNECTOR_CLASS))
                .forEach((key, value) -> {
                    try {
                        value.ensureValid(key, connector.getSpec().getConfig().get(key));
                    } catch (FieldValidationException e) {
                        validationErrors.add(e.getMessage());
                    }
                });
        }
        return validationErrors;
    }
}
