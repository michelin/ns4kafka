package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.ActionDefinition;
import io.micronaut.core.annotation.Introspected;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class ApiResourcesService {

    @Getter
    List<ActionDefinition> actionDefinitions = List.of(
            ActionDefinition.builder()
                    .kind("Connector")
                    .path("restart")
                    .names("restart")
                    .mustHaveResourceName(true)
                    .build()
    );

    public List<ActionDefinition> getActionsOfResource(String resource) {
        return getActionDefinitions().stream()
                .filter(definition -> definition.getKind().equals(resource))
                .collect(Collectors.toList());
    }
}