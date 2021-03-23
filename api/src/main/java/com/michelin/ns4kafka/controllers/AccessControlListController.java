package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.*;
import java.util.stream.Collectors;

@Tag(name = "Cross Namespace Topic Grants",
        description = "APIs to handle cross namespace ACL")
@Controller("/api/namespaces/{namespace}/acls")
public class AccessControlListController {
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    @Operation(summary = "Returns the Access Control Entry List")
    @Get("{?limit}")
    public List<AccessControlEntry> list(String namespace, Optional<AclLimit> limit){
        if(limit.isEmpty())
            limit = Optional.of(AclLimit.ALL);

        Namespace myNamespace = namespaceRepository.findByName(namespace)
                .orElseThrow(() -> new RuntimeException("Namespace not found :"+namespace));


        switch (limit.get()){
            case GRANTEE:
                return accessControlEntryRepository.findAllGrantedToNamespace(namespace)
                        .stream()
                        // granted to me
                        .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                        // excepted by admin
                        .filter(accessControlEntry -> !accessControlEntry.getMetadata().getNamespace().equals(namespace))
                        .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                        .collect(Collectors.toList());
            case GRANTOR:
                return accessControlEntryRepository.findAllForCluster(myNamespace.getCluster())
                        .stream()
                        // granted by me (including my own)
                        .filter(accessControlEntry -> accessControlEntry.getMetadata().getNamespace().equals(namespace))
                        // without the granted to me
                        .filter(accessControlEntry -> !accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                        .sorted(Comparator.comparing(o -> o.getSpec().getGrantedTo()))
                        .collect(Collectors.toList());
            case ALL:
            default:
                return accessControlEntryRepository.findAllForCluster(myNamespace.getCluster())
                        .stream()
                        .filter(accessControlEntry ->
                                accessControlEntry.getMetadata().getNamespace().equals(namespace)
                                ||  accessControlEntry.getSpec().getGrantedTo().equals(namespace)
                        )
                        .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                        .collect(Collectors.toList());
        }

    }

    @Post()
    public AccessControlEntry grantACL(String namespace, @Valid @Body AccessControlEntry accessControlEntry){
        // 1. (Done) User Allowed ?
        //   -> User belongs to group and operation/resource is allowed on this namespace ?
        //   -> Managed in RessourceBasedSecurityRule class
        // 3. Request Valid ?
        //   -> AccessControlEntry parameters are valid
        //   -> Target namespace exists ? should it be checked ?
        // 4. Store in datastore

        // 3. Validate the AccessControlEntry object
        List<String> validationErrors = new ArrayList<>();



        // TODO generate allowedLists from config ?
        // Which resource can be granted cross namespaces ? TOPIC
        List<AccessControlEntry.ResourceType> allowedResourceTypes =
                List.of(AccessControlEntry.ResourceType.TOPIC);
        // Which permission can be granted cross namespaces ? READ, WRITE
        // Only admin can grant OWNER
        List<AccessControlEntry.Permission> allowedPermissions =
                List.of(AccessControlEntry.Permission.READ,
                        AccessControlEntry.Permission.WRITE);
        // Which patternTypes can be granted
        List<AccessControlEntry.ResourcePatternType> allowedPatternTypes =
                List.of(AccessControlEntry.ResourcePatternType.LITERAL,
                        AccessControlEntry.ResourcePatternType.PREFIXED);

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
        if(!allowedPatternTypes.contains(accessControlEntry.getSpec().getResourcePatternType())){
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getResourcePatternType() +
                    " for patternType: Value must be one of [" +
                    allowedPatternTypes.stream().map(Object::toString).collect(Collectors.joining(", ")) +
                    "]");
        }


        // GrantedTo Namespace exists ?
        Optional<Namespace> optionalNamespace = namespaceRepository.findByName(accessControlEntry.getSpec().getGrantedTo());
        if(optionalNamespace.isEmpty()){
            validationErrors.add("Invalid value "+accessControlEntry.getSpec().getGrantedTo()+" for grantedTo: Namespace doesn't exist");
        }

        // Are you dumb ?
        if(namespace.equals(accessControlEntry.getSpec().getGrantedTo())){
            validationErrors.add("Invalid value "+accessControlEntry.getSpec().getGrantedTo()+" for grantedTo: Why would you grant to yourself ?!");
        }

        // Grantor Namespace is OWNER of Resource + ResourcePattern ?
        boolean isOwner = accessControlEntryRepository.findAllGrantedToNamespace(namespace).stream()
                .filter(ace -> ace.getSpec().getResourceType() == accessControlEntry.getSpec().getResourceType() &&
                        ace.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .anyMatch(ace -> {
                    // if grantor is owner of PREFIXED resource that starts with
                    // owner  PREFIXED: priv_bsm_
                    // grants LITERAL : priv_bsm_topic  OK
                    // grants PREFIXED: priv_bsm_topic  OK
                    // grants PREFIXED: priv_b          NO
                    // grants LITERAL : priv_b          NO
                    // grants PREFIXED: priv_bsm_       OK
                    // grants LITERAL : pric_bsm_       OK
                    if(ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED &&
                            accessControlEntry.getSpec().getResource().startsWith(ace.getSpec().getResource())) {
                        // if so, either patternType are fine (LITERAL/PREFIXED)
                        return true;
                    }
                    // if grantor is owner of LITERAL resource :
                    // exact match to LITERAL grant
                    // owner  LITERAL : priv_bsm_topic
                    // grants LITERAL : priv_bsm_topic  OK
                    // grants PREFIXED: priv_bsm_topic  NO
                    // grants PREFIXED: priv_bs         NO
                    // grants LITERAL : priv_b          NO
                    // grants PREFIXED: priv_bsm_topic2 NO
                    // grants LITERAL : pric_bsm_topic2 NO
                    if(ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.LITERAL &&
                            accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.LITERAL &&
                            accessControlEntry.getSpec().getResource().equals(ace.getSpec().getResource())) {
                        return true;
                    }
                    return false;
                });
        if(!isOwner){
            validationErrors.add("Invalid grant "+accessControlEntry.getSpec().getResourcePatternType()+":"+
                    accessControlEntry.getSpec().getResource()+
                    " : Namespace is neither OWNER of LITERAL:resource nor top-level PREFIXED:resource");
        }

        if(validationErrors.size()>0){
            throw new ResourceValidationException(validationErrors);
        }

        //TODO handle already exists ?

        accessControlEntry.setMetadata(ObjectMeta.builder()
                .cluster(optionalNamespace.get().getCluster())
                .namespace(namespace)
                .labels(Map.of("grantedBy",namespace))
                .build());

        return accessControlEntryRepository.create(accessControlEntry);
    }

    @Delete("/{name}")
    @Status(HttpStatus.NO_CONTENT)
    public void revokeACL(String namespace, String name){

        // 1. Check Ownership of ACL using metadata.namespace
        // 2. Check ACL doesn't apply to me ? why would you drop your own rights ?!
        // TODO Fix 1. + 2. with metadata.namespace: "admin" when initializing a namespace
        // 3. Drop ACL
        List<String> validationErrors = new ArrayList<>();

        Optional<AccessControlEntry> optionalAccessControlEntry = accessControlEntryRepository.findByName(namespace, name);
        if(optionalAccessControlEntry.isEmpty()){
            validationErrors.add("Invalid value "+name+" for name : AccessControlEntry doesn't exist");
        }else{
            if(optionalAccessControlEntry.get().getSpec().getGrantedTo().equals(namespace)){
                validationErrors.add("Invalid value "+name + " for name: Why would you revoke to yourself ?!");
            }
        }

        if(validationErrors.size()>0){
            throw new ResourceValidationException(validationErrors);
        }

        accessControlEntryRepository.deleteByName(name);
    }

    public enum AclLimit {
        /** Returns all ACL */
        ALL,
        GRANTOR,
        GRANTEE
    }
}
