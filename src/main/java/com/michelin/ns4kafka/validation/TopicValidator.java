package com.michelin.ns4kafka.validation;

import com.michelin.ns4kafka.models.Topic;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@NoArgsConstructor
@Getter
@Setter
public class TopicValidator extends ResourceValidator {

    @Builder
    public TopicValidator(Map<String, ValidationConstraint> validationConstraints){
        super(validationConstraints);
    }
    public void validate(Topic topic){
        //unkonwn configs ? forbidden.
        Set<String> configsWithoutConstraints = topic.getSpec().getConfigs().keySet()
                .stream()
                .filter(s -> !validationConstraints.containsKey(s))
                .collect(Collectors.toSet());
        //TODO return list of Violation instead of Exception
        if(configsWithoutConstraints.size()>0){
            throw new ValidationException("Configurations ["+String.join(",",configsWithoutConstraints)+"] are not allowed");
        }
        if(validationConstraints.containsKey("partitions")){
            validationConstraints.get("partitions").getValidator().ensureValid("partitions", topic.getSpec().getPartitions());
        }
        if(validationConstraints.containsKey("replication.factor")){
            validationConstraints.get("replication.factor").getValidator().ensureValid("replication.factor", topic.getSpec().getReplicationFactor());
        }


    }

}
