package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.validation.FieldValidationException;
import com.michelin.ns4kafka.validation.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.validation.validator.Validator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.kafka.common.quota.ClientQuotaAlteration;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tag(name = "Cross Namespace Topic Grants",
        description = "APIs to handle cross namespace ACL")
@Controller("/api/namespaces/{namespace}/acls")
public class AccessControlListController {
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    @Operation(summary = "Returns the Access Control Entry List")
    @Get("/{?limit}")
    public List<AccessControlEntry> list(String namespace, Optional<AclLimit> limit){
        if(limit.isEmpty())
            limit = Optional.of(AclLimit.ALL);

        Namespace myNamespace = namespaceRepository.findByName(namespace)
                .orElseThrow(() -> new RuntimeException("Namespace not found :"+namespace));


        switch (limit.get()){
            case GRANTEE:
                return accessControlEntryRepository.findAllGrantedToNamespace(myNamespace.getName())
                        .stream()
                        .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                        .collect(Collectors.toList());
            case GRANTOR:
                return accessControlEntryRepository.findAllForCluster(myNamespace.getCluster())
                        .stream()
                        .filter(accessControlEntry -> accessControlEntry.getMetadata().getLabels().get("grantedBy").equals(myNamespace.getName()))
                        .sorted(Comparator.comparing(o -> o.getSpec().getGrantedTo()))
                        .collect(Collectors.toList());
            case ALL:
            default:
                return accessControlEntryRepository.findAllForCluster(myNamespace.getCluster())
                        .stream()
                        .filter(accessControlEntry ->
                                accessControlEntry.getMetadata().getNamespace().equals(myNamespace.getName()))
                        .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                        .collect(Collectors.toList());
        }

    }

    @Post()
    public List<AccessControlEntry> grantACL(String namespace, @Valid @Body AccessControlEntry accessControlEntry){
        //TODO
        // 1. (Done) User Allowed ?
        //   -> User belongs to group and operation/resource is allowed on this namespace ?
        //   -> Managed in RessourceBasedSecurityRule class
        // 3. Request Valid ?
        //   -> AccessControlEntry parameters are valid
        //   - >
        //   -> Target namespace exists ? should it be checked ?
        // 4. Store in datastore

        // 3. Validate the AccessControlEntry object
        List<String> validationErrors = new ArrayList<>();



        // TODO generate allowedLists from config ?
        // Which resource can be granted cross namespaces ? TOPIC
        List<AccessControlEntry.ResourceType> allowedResourceTypes =
                List.of(AccessControlEntry.ResourceType.TOPIC);
        // Which permission can be granted cross namespaces ? READ, WRITE
        List<AccessControlEntry.Permission> allowedPermissions =
                List.of(AccessControlEntry.Permission.READ,
                        AccessControlEntry.Permission.WRITE);

        if(!allowedResourceTypes.contains(accessControlEntry.getSpec().getResourceType())){
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getResourceType() +
                    " for resourceType: Value must be one of [" +
                    allowedResourceTypes.stream().map(Object::toString).collect(Collectors.joining(", ")) +
                    "]");
        }
        if(!allowedPermissions.contains(accessControlEntry.getSpec().getPermission())){

            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getPermission() +
                    " for permission: Value must be one of [" +
                    allowedPermissions.stream().map(Object::toString).collect(Collectors.joining(", ")) +
                    "]");
        }

        // Grantee Namespace is set (and exists ?) ?
        if(accessControlEntry.getSpec().getGrantedTo() == null || accessControlEntry.getMetadata().getNamespace().trim().isEmpty()){
            validationErrors.add("Invalid value null for grantedTo");
        }else{
            Optional<Namespace> optionalNamespace = namespaceRepository.findByName(accessControlEntry.getMetadata().getNamespace());
            //TODO should we ?
            if(optionalNamespace.isEmpty()){
                validationErrors.add("Invalid value "+accessControlEntry.getSpec().getGrantedTo()+" for grantedTo: Namespace doesn't exist");
            }
        }
        // Grantor Namespace is OWNER of Resource + ResourcePattern ?


        if(validationErrors.size()>0){
            throw new ResourceValidationException(validationErrors);
        }

        return List.of(accessControlEntry);
    }
    @Delete("/{namespace2}/")
    public HttpResponse revokeACL(String namespace, String namespace2){
        return HttpResponse.accepted();
    }

    public enum AclLimit {
        /** Returns all ACL */
        ALL,
        GRANTOR,
        GRANTEE
    }
}
