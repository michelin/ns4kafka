package com.michelin.ns4kafka.validation;

import com.michelin.ns4kafka.models.connector.Connector;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.michelin.ns4kafka.utils.config.ConnectorConfig.CONNECTOR_CLASS;

@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper=true)
public class ConnectValidator extends ResourceValidator {
    @Builder.Default
    private Map<String, Validator> sourceValidationConstraints = new HashMap<>();

    @Builder.Default
    private Map<String, Validator> sinkValidationConstraints = new HashMap<>();

    @Builder.Default
    private Map<String, Map<String, Validator>> classValidationConstraints = new HashMap<>();

    /**
     * Validate a given connector
     * @param connector The connector
     * @param connectorType The connector type
     * @return A list of validation errors
     */
    public List<String> validate(Connector connector, String connectorType) {
        List<String> validationErrors = new ArrayList<>();

        if (connector.getMetadata().getName().isEmpty()) {
            validationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Value must not be empty");
        }

        if (connector.getMetadata().getName().length() > 249) {
            validationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Value must not be longer than 249");
        }

        if (!connector.getMetadata().getName().matches("[a-zA-Z0-9._-]+")) {
            validationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Value must only contain " +
                    "ASCII alphanumerics, '.', '_' or '-'");
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
            classValidationConstraints.get(connector.getSpec().getConfig().get(CONNECTOR_CLASS)).forEach((key, value) -> {
                try {
                    value.ensureValid(key, connector.getSpec().getConfig().get(key));
                } catch (FieldValidationException e) {
                    validationErrors.add(e.getMessage());
                }
            });
        }
        return validationErrors;
    }

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
}
