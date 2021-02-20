package com.michelin.ns4kafka.validation;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@NoArgsConstructor
@Getter
@Setter
public class ConnectValidator extends ResourceValidator{

    //constraints applies to all connectors
    // key.converter
    // value.converter
    Map<String, Validator> globalValidationConstraints;
    Map<String, Validator> sourceValidationConstraints;
    // For Sinks, error handling is probably a good mandatory choice :
    // - errors.tolerance non null
    // - errors.deadletterqueue.topic.name non blank
    // Override user is probably a good choice too :
    // - consumer.override.sasl.jaas.config
    Map<String, Validator> sinkValidationConstraints;

    // validation step for class specific Connectors
    // ie. for io.confluent.connect.jdbc.JdbcSinkConnector :
    // - db.timezone non Blank before they complain that time is not right in their DB
    Map<String,Map<String, Validator>> classValidationConstraints;

    @Builder
    public ConnectValidator(Map<String, Validator> validationConstraints){
        super(validationConstraints);
    }

    public List<String> validate(Connector connector, Namespace namespace){
        List<String> validationErrors = new ArrayList<>();

        if(connector.getMetadata().getName().isEmpty())
            validationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Value must not be empty");
        if (connector.getMetadata().getName().length() > 249)
            validationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Value must not be longer than 249");
        if (!connector.getMetadata().getName().matches("[a-zA-Z0-9._-]+"))
            validationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Value must only contain " +
                    "ASCII alphanumerics, '.', '_' or '-'");


        //validate configurations
        validationConstraints.entrySet().stream().forEach(entry -> {
            try {
                entry.getValue().ensureValid(entry.getKey(), connector.getSpec().get(entry.getKey()));
            } catch (FieldValidationException e){
                validationErrors.add(e.getMessage());
            }
        });
        return validationErrors;

    }
}
