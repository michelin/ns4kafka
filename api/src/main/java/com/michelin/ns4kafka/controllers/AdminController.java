package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.LoggerFactory;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Controller("/api/admin")
public class AdminController {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AdminController.class);

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;
    @Inject
    RoleBindingRepository roleBindingRepository;
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;

    @Delete("/acl/{acl}")
    @Status(HttpStatus.NO_CONTENT)
    public void deleteACL(String acl){
        accessControlEntryRepository.deleteByName(acl);
    }
    @Delete("/namespace/{namespace}")
    public List<Object> deleteNamespace(String namespace){
        Optional<Namespace> namespaceOptional = namespaceRepository.findByName(namespace);
        if(namespaceOptional.isPresent()){

            List<AccessControlEntry> accessControlEntryList = accessControlEntryRepository.findAllGrantedToNamespace(namespace);
            List<RoleBinding> roleBindingList = roleBindingRepository.findAllForNamespace(namespace);

            namespaceRepository.delete(namespaceOptional.get());
            accessControlEntryList.forEach(accessControlEntry -> accessControlEntryRepository.deleteByName(accessControlEntry.getMetadata().getName()));
            roleBindingList.forEach(roleBinding -> roleBindingRepository.delete(roleBinding));

            List<Object> returnList = new ArrayList<>();
            returnList.add(namespaceOptional.get());
            returnList.addAll(accessControlEntryList);
            returnList.addAll(roleBindingList);
            return returnList;
        }
        return List.of();
    }

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
        //Prepare Namespace
        Namespace toCreate = Namespace.builder()
                .name(namespaceCreationRequest.getName())
                .cluster(namespaceCreationRequest.getCluster())
                .defaulKafkatUser(namespaceCreationRequest.getKafkaUser())
                .topicValidator(TopicValidator.makeDefault())
                .connectValidator(ConnectValidator.makeDefault())
                .build();
        // Prepare ACLs
        List<AccessControlEntry> accessControlEntryList = AccessControlEntry.buildGeneric(
                namespaceCreationRequest.getName(),
                namespaceCreationRequest.getCluster(),
                namespaceCreationRequest.getPrefix());

        //Store Namespace and ACLs;
        Namespace created = namespaceRepository.createNamespace(toCreate);
        roleBindingRepository.create(new RoleBinding(namespaceCreationRequest.getName(),namespaceCreationRequest.getGroup()));
        accessControlEntryList.forEach(accessControlEntry -> {
            LOG.info(accessControlEntry.getSpec().toString());
            accessControlEntryRepository.create(accessControlEntry);

        });
        return  created;
    }


    @Introspected
    @Getter
    @Setter
    @NoArgsConstructor
    public static class NamespaceCreationRequest{
        @NotBlank
        String name;
        @NotBlank
        String group;
        @NotBlank
        String cluster;
        @NotBlank
        String kafkaUser;
        @NotBlank
        String prefix;

    }
}
