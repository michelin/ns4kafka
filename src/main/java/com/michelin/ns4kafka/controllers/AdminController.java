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
import com.michelin.ns4kafka.security.RessourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NewNamespaceValidator;
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

@RolesAllowed(RessourceBasedSecurityRule.IS_ADMIN)
@Controller("/api/admin")
public class AdminController {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AdminController.class);

    @Inject
    NewNamespaceValidator newNamespaceValidator;
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;
    @Inject
    RoleBindingRepository roleBindingRepository;

    @Delete("/acl/{acl}")
    @Status(HttpStatus.NO_CONTENT)
    public void deleteACL(String acl) {
        accessControlEntryRepository.deleteByName(acl);
    }

    @Delete("/namespace/{namespace}")
    public List<Object> deleteNamespace(String namespace) {
        Optional<Namespace> namespaceOptional = namespaceRepository.findByName(namespace);
        if (namespaceOptional.isPresent()) {

            List<AccessControlEntry> accessControlEntryList = accessControlEntryRepository
                    .findAllGrantedToNamespace(namespace);
            List<RoleBinding> roleBindingList = roleBindingRepository.findAllForNamespace(namespace);

            namespaceRepository.delete(namespaceOptional.get());
            accessControlEntryList.forEach(accessControlEntry -> accessControlEntryRepository
                    .deleteByName(accessControlEntry.getMetadata().getName()));
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
    public Namespace createNamespace(@Valid @Body NamespaceCreationRequest namespaceCreationRequest) {

        if (!newNamespaceValidator.validate(namespaceCreationRequest)) {
        }
        // Prepare Namespace
        Namespace toCreate = Namespace.builder().name(namespaceCreationRequest.getName())
                .cluster(namespaceCreationRequest.getCluster())
                .defaulKafkatUser(namespaceCreationRequest.getKafkaUser()).topicValidator(TopicValidator.makeDefault())
                .connectValidator(ConnectValidator.makeDefault()).build();
        // Prepare ACLs
        List<AccessControlEntry> accessControlEntryList = AccessControlEntry.buildGeneric(
                namespaceCreationRequest.getName(), namespaceCreationRequest.getCluster(),
                namespaceCreationRequest.getPrefix());

        // Store Namespace and ACLs;
        Namespace created = namespaceRepository.createNamespace(toCreate);
        roleBindingRepository
                .create(new RoleBinding(namespaceCreationRequest.getName(), namespaceCreationRequest.getGroup()));
        accessControlEntryList.forEach(accessControlEntry -> {
            LOG.info(accessControlEntry.getSpec().toString());
            accessControlEntryRepository.create(accessControlEntry);

        });
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
