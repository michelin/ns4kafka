package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.security.ResourceSecurityPolicy;
import com.michelin.ns4kafka.models.security.TopicSecurityPolicy;
import io.micronaut.security.utils.SecurityService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
public class NamespaceRepository {
    private List<Namespace> namespaces;
    @Inject
    private SecurityService securityService;

    public NamespaceRepository(){
        Namespace n1 = new Namespace();
        n1.setName("rf4maze0");
        n1.setOwner("f4m/admins");
        n1.setDiskQuota(5);
        n1.setPolicies(List.of(new TopicSecurityPolicy("f4m.*", ResourceSecurityPolicy.ResourcePatternType.PREFIXED,ResourceSecurityPolicy.SecurityPolicy.OWNER)));

        Namespace n2 = new Namespace();
        n2.setName("rsebaze0");
        n2.setOwner("GP-MIC-SEB-DEVOPS");
        n2.setDiskQuota(10);
        n2.setPolicies(List.of(new TopicSecurityPolicy("seb.*", ResourceSecurityPolicy.ResourcePatternType.PREFIXED,ResourceSecurityPolicy.SecurityPolicy.OWNER)));

        namespaces = List.of(n1,n2);
    }

    public List<Namespace> findAll(){
        return namespaces.stream()
                .filter(ns -> securityService.hasRole(ns.getOwner()))
                .collect(Collectors.toList());
    }

    public Optional<Namespace> findByName(String namespace) {
        return namespaces.stream()
                .filter(ns -> securityService.hasRole(ns.getOwner()) && ns.getName().equals(namespace))
                .findFirst();
    }
}
