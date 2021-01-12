package com.michelin.ns4kafka.validation;

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
    public List<String> validate(Topic topic){
        List<String> validationErrors = new ArrayList<>();

        //unkonwn configs ? forbidden.
        Set<String> configsWithoutConstraints = topic.getSpec().getConfigs().keySet()
                .stream()
                .filter(s -> !validationConstraints.containsKey(s))
                .collect(Collectors.toSet());
        if(configsWithoutConstraints.size()>0){
            validationErrors.add("configurations ["+String.join(",",configsWithoutConstraints)+"] are not allowed");
        }


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
            }catch (ValidationException e){
                validationErrors.add(e.getMessage());
            }
        });
        return validationErrors;
    }

}
