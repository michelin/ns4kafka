package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.validation.ResourceValidationException;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Controller("/api/admin")
public class AdminController {

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;

    @RolesAllowed("isAdmin()")
    @Post("/namespace")
    public Namespace createNamespace(@Valid @Body NamespaceCreationRequest namespaceCreationRequest){

        // Validation steps:
        // - namespace must not already exist
        // - cluster must exist
        // - kafkaUser must not exist within the namespaces linked to this cluster
        // - prefix ? prefix overlap ? "seb" currently exists and we try to create "se" or "seb_a"
        //      current     new     check
        //      seb         seb_a   new.startswith(current)
        //      seb         se      current.startswith(new)
        List<String> validationErrors = new ArrayList<>();
        if(namespaceRepository.findByName(namespaceCreationRequest.getName()).isPresent()) {
            validationErrors.add("Namespace already exist");
        }

        if(kafkaAsyncExecutorConfigList.stream()
                .noneMatch(config -> config.getName().equals(namespaceCreationRequest.getCluster()))) {
            validationErrors.add("Cluster doesn't exist");
        }
        if(namespaceRepository.findAllForCluster(namespaceCreationRequest.getCluster()).stream()
                .anyMatch(namespace -> namespace.getDefaulKafkatUser().equals(namespaceCreationRequest.getKafkaUser()))){
            validationErrors.add("KafkaUser already exist");
        }
        List<AccessControlEntry> prefixInUse = accessControlEntryRepository.findAllForCluster(namespaceCreationRequest.getCluster()).stream()
                .filter(ace -> ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED)
                .filter(ace -> ace.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC)
                .filter(ace -> ace.getSpec().getResource().startsWith(namespaceCreationRequest.getPrefix())
                        || namespaceCreationRequest.getPrefix().startsWith(ace.getSpec().getResource()))
            .collect(Collectors.toList());
        if(prefixInUse.size()>0) {
            validationErrors.add(String.format("Prefix overlaps with namespace %s: [%s]"
                    , prefixInUse.get(0).getSpec().getGrantedTo()
                    , prefixInUse.get(0).getSpec().getResource()));
        }


        if(validationErrors.size()>0){
            throw new ResourceValidationException(validationErrors);
        }
        //TODO this
        Namespace toCreate = Namespace.builder().build();
        return  toCreate;
    }


    @Introspected
    @Getter
    @Setter
    @NoArgsConstructor
    public static class NamespaceCreationRequest{
        @NotBlank
        String name;
        @NotBlank
        String cluster;
        @NotBlank
        String kafkaUser;
        @NotBlank
        String prefix;

    }
}
