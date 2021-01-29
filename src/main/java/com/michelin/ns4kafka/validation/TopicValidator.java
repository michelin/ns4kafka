package com.michelin.ns4kafka.validation;

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
public class TopicValidator extends ResourceValidator {

    @Builder
    public TopicValidator(Map<String, Validator> validationConstraints){
        super(validationConstraints);
    }

    public List<String> validate(Topic topic, Namespace namespace){
        List<String> validationErrors = new ArrayList<>();

        //Topic name validation
        //https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L36
        if(topic.getMetadata().getName().isEmpty())
            validationErrors.add("Invalid value " + topic.getMetadata().getName() + " for name: Value must not be empty");
        if (topic.getMetadata().getName().equals(".") || topic.getMetadata().getName().equals(".."))
            validationErrors.add("Invalid value " + topic.getMetadata().getName() + " for name: Value must not be \".\" or \"..\"");
        if (topic.getMetadata().getName().length() > 249)
            validationErrors.add("Invalid value " + topic.getMetadata().getName() + " for name: Value must not be longer than 249");
        if (!topic.getMetadata().getName().matches("[a-zA-Z0-9._-]+"))
            validationErrors.add("Invalid value " + topic.getMetadata().getName() + " for name: Value must only contain " +
                        "ASCII alphanumerics, '.', '_' or '-'");

        //prevent unknown configurations
        if(topic.getSpec().getConfigs() != null) {
            Set<String> configsWithoutConstraints = topic.getSpec().getConfigs().keySet()
                    .stream()
                    .filter(s -> !validationConstraints.containsKey(s))
                    .collect(Collectors.toSet());
            if (configsWithoutConstraints.size() > 0) {
                validationErrors.add("Configurations [" + String.join(",", configsWithoutConstraints) + "] are not allowed");
            }
        }
        //validate configurations
        validationConstraints.entrySet().stream().forEach(entry -> {
            try {
                //TODO move from exception based to list based ?
                //partitions and rf
                if (entry.getKey().equals("partitions")) {
                    entry.getValue().ensureValid(entry.getKey(), topic.getSpec().getPartitions());
                } else if (entry.getKey().equals("replication.factor")) {
                    entry.getValue().ensureValid(entry.getKey(), topic.getSpec().getReplicationFactor());
                } else {
                    //TODO null check on topic.getSpec().getConfigs() before reaching this code ?
                    // are there use-cases without any validation on configs ?
                    // if so, configs should be allowed to be null/empty
                    if(topic.getSpec().getConfigs() != null) {
                        entry.getValue().ensureValid(entry.getKey(), topic.getSpec().getConfigs().get(entry.getKey()));
                    }else{
                        validationErrors.add("Invalid value null for configuration "+entry.getKey()+": Value must be non-null");
                    }
                }
            }catch (FieldValidationException e){
                validationErrors.add(e.getMessage());
            }
        });
        return validationErrors;
    }

}
