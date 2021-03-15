package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.security.rules.SecurityRuleResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
public class ResourceBasedSecurityRuleTest {
    @Mock
    NamespaceRepository namespaceRepository;
    @Mock
    RoleBindingRepository roleBindingRepository;

    @InjectMocks
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @Test
    void CheckReturnsUnknown_Unauthenticated(){
        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/anything"),null,null);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void CheckReturnsUnknown_InvalidResource(){
        List<String> groups = List.of("group1");
        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/non-namespaced/resource"),null,
                Map.of("sub","user", "groups", groups));
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void CheckReturnsUnknown_NoRoleBinding(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups);
        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of());

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/test/connects"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void CheckReturnsUnknown_InvalidNamespace(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups);
        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of(new RoleBinding("test","group1")));
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.empty());

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/test/connects"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void CheckReturnsAllowed(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups);
        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of(new RoleBinding("test","group1")));
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/test/connects"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

}
