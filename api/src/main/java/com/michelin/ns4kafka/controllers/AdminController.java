package com.michelin.ns4kafka.controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;

import org.slf4j.LoggerFactory;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Controller("/api/admin")
public class AdminController {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AdminController.class);

    @Inject
    NamespaceService namespaceService;
    @Inject
    AccessControlEntryService accessControlEntryService;
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    RoleBindingRepository roleBindingRepository;

    @Delete("/acl/{acl}")
    @Status(HttpStatus.NO_CONTENT)
    public void deleteACL(String acl) {
        accessControlEntryService.deleteByName(acl);
    }

    @Delete("/namespace/{namespace}")
    public List<Object> deleteNamespace(String namespace) {
        Optional<Namespace> namespaceOptional = namespaceRepository.findByName(namespace);
        if (namespaceOptional.isPresent()) {

            List<RoleBinding> roleBindingList = roleBindingRepository.findAllForNamespace(namespace);

            namespaceRepository.delete(namespaceOptional.get());
            roleBindingList.forEach(roleBinding -> roleBindingRepository.delete(roleBinding));

            List<Object> returnList = new ArrayList<>();
            returnList.add(namespaceOptional.get());
            returnList.addAll(accessControlEntryService.deleteACLAttachedToNamespace(namespace));
            returnList.addAll(roleBindingList);
            return returnList;
        }
        return List.of();
    }

    @Post("/namespace")
    public Namespace createNamespace(@Valid @Body NamespaceCreationRequest namespaceCreationRequest) {
        // Validation steps:
        // - namespace must not already exist
        // - cluster must exist
        // - kafkaUser must not exist within the namespaces linked to this cluster
        // - prefix ? prefix overlap ? "seb" currently exists and we try to create "se" or "seb_a"
        // current   new     check
        //   seb    seb_a    new.startswith(current)
        //   seb    se       current.startswith(new)
        List<String> validationError = new ArrayList<>();
        validationError.addAll(namespaceService.validate(namespaceCreationRequest));
        validationError.addAll(accessControlEntryService.prefixInUse(namespaceCreationRequest));
        if (!validationError.isEmpty()) {
            throw new ResourceValidationException(validationError);
        }
        // Prepare Namespace
        Namespace toCreate = Namespace.builder().name(namespaceCreationRequest.getName())
                .cluster(namespaceCreationRequest.getCluster())
                .defaulKafkatUser(namespaceCreationRequest.getKafkaUser())
                .topicValidator(TopicValidator.makeDefault())
                .connectValidator(ConnectValidator.makeDefault()).build();
        // Prepare ACLs
        List<AccessControlEntry> accessControlEntryList = AccessControlEntry.buildGeneric(
                namespaceCreationRequest.getName(), namespaceCreationRequest.getCluster(),
                namespaceCreationRequest.getPrefix());

        // Store Namespace and ACLs;
        Namespace created = namespaceRepository.createNamespace(toCreate);
        roleBindingRepository
                .create(new RoleBinding(namespaceCreationRequest.getName(), namespaceCreationRequest.getGroup()));
        accessControlEntryService.create(accessControlEntryList);
        return created;
    }

    @Introspected
    @Getter
    @Setter
    @NoArgsConstructor
    public static class NamespaceCreationRequest {
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
