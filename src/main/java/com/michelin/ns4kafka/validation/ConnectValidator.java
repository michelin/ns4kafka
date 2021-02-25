package com.michelin.ns4kafka.validation;

import com.michelin.ns4kafka.models.Connector;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
@Getter
@Setter
public class ConnectValidator extends ResourceValidator{

    //constraints applies to all connectors
    // key.converter
    // value.converter
    //Map<String, Validator> globalValidationConstraints;
    Map<String, Validator> sourceValidationConstraints;
    // For Sinks, error handling is probably a good mandatory choice :
    // - errors.tolerance non null
    // - errors.deadletterqueue.topic.name non blank
    // Override user is probably a good choice too :
    // - consumer.override.sasl.jaas.config
    Map<String, Validator> sinkValidationConstraints;

    // validation step for class specific Connectors
    // ie. for io.confluent.connect.jdbc.JdbcSinkConnector :
    // - db.timezone non Blank before they complain that time is not right in their DBcap
    Map<String,Map<String, Validator>> classValidationConstraints;

    @Builder
    public ConnectValidator(Map<String, Validator> validationConstraints,
                            Map<String, Validator> sourceValidationConstraints,
                            Map<String, Validator> sinkValidationConstraints,
                            Map<String,Map<String, Validator>> classValidationConstraints
    ){
        super(validationConstraints);
        this.sourceValidationConstraints = sourceValidationConstraints;
        this.sinkValidationConstraints = sinkValidationConstraints;
        this.classValidationConstraints = classValidationConstraints;
    }

    public List<String> validate(Connector connector, String connectorType){
        List<String> validationErrors = new ArrayList<>();

        if(connector.getMetadata().getName().isEmpty())
            validationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Value must not be empty");
        if (connector.getMetadata().getName().length() > 249)
            validationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Value must not be longer than 249");
        if (!connector.getMetadata().getName().matches("[a-zA-Z0-9._-]+"))
            validationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Value must only contain " +
                    "ASCII alphanumerics, '.', '_' or '-'");

        //validate constraints
        validationConstraints.entrySet().stream().forEach(entry -> {
            try {
                entry.getValue().ensureValid(entry.getKey(), connector.getSpec().get(entry.getKey()));
            } catch (FieldValidationException e){
                validationErrors.add(e.getMessage());
            }
        });
        if(connectorType.equals("sink")){
            sinkValidationConstraints.entrySet().stream().forEach(entry -> {
                try {
                    entry.getValue().ensureValid(entry.getKey(), connector.getSpec().get(entry.getKey()));
                } catch (FieldValidationException e){
                    validationErrors.add(e.getMessage());
                }
            });
        }
        if(connectorType.equals("source"))
            sourceValidationConstraints.entrySet().stream().forEach(entry -> {
                try {
                    entry.getValue().ensureValid(entry.getKey(), connector.getSpec().get(entry.getKey()));
                } catch (FieldValidationException e){
                    validationErrors.add(e.getMessage());
                }
            });
        if(classValidationConstraints.containsKey(connector.getSpec().get("connector.class"))){
            classValidationConstraints.get(connector.getSpec().get("connector.class")).entrySet().stream().forEach(entry -> {
                try {
                    entry.getValue().ensureValid(entry.getKey(), connector.getSpec().get(entry.getKey()));
                } catch (FieldValidationException e){
                    validationErrors.add(e.getMessage());
                }
            });
        }
        return validationErrors;
    }

    public static ConnectValidator makeDefault(){
        return ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter", new ResourceValidator.NonEmptyString(),
                        "value.converter", new ResourceValidator.NonEmptyString(),
                        "connector.class", new ResourceValidator.ValidString(
                                List.of("io.confluent.connect.jdbc.JdbcSourceConnector",
                                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                                        "com.splunk.kafka.connect.SplunkSinkConnector",
                                        "org.apache.kafka.connect.file.FileStreamSinkConnector")
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
