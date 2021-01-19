package com.michelin.ns4kafka.validation;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.security.ResourceSecurityPolicyValidator;
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
            validationErrors.add("Invalid value " + topic.getMetadata().getName() + ": Topic Name can't be empty");
        if (topic.getMetadata().getName().equals(".") || topic.getMetadata().getName().equals(".."))
            validationErrors.add("Invalid value " + topic.getMetadata().getName() + ": Topic name can't be \".\" or \"..\"");
        if (topic.getMetadata().getName().length() > 249)
            validationErrors.add("Invalid value " + topic.getMetadata().getName() + ": Topic name can't be longer than 249");
        if (!topic.getMetadata().getName().matches("[a-zA-Z0-9._-]+"))
            validationErrors.add("Invalid value " + topic.getMetadata().getName() + ": Topic name contains a character other than " +
                        "ASCII alphanumerics, '.', '_' and '-'");

        //Topic namespace ownership validation
        if(!ResourceSecurityPolicyValidator.isNamespaceOwnerOnTopic(namespace,topic))
            validationErrors.add("Invalid value " + topic.getMetadata().getName() + ": Topic Name not allowed for this namespace");

        //prevent unknown configurations
        Set<String> configsWithoutConstraints = topic.getSpec().getConfigs().keySet()
                .stream()
                .filter(s -> !validationConstraints.containsKey(s))
                .collect(Collectors.toSet());
        if(configsWithoutConstraints.size()>0){
            validationErrors.add("Configurations ["+String.join(",",configsWithoutConstraints)+"] are not allowed");
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
                    entry.getValue().ensureValid(entry.getKey(), topic.getSpec().getConfigs().get(entry.getKey()));
                }
            }catch (FieldValidationException e){
                validationErrors.add(e.getMessage());
            }
        });
        return validationErrors;
    }

}
