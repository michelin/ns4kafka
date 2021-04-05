package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.http.HttpRequest;
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
    void CheckReturnsUnknown_MissingClaims(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups);
        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/anything"),null,claims);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void CheckReturnsUnknown_InvalidResource(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/non-namespaced/resource"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void CheckReturnsUnknown_NoRoleBinding(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of());

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/test/connects"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void CheckReturnsUnknown_InvalidNamespace(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.empty());

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/test/connects"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }
    @Test
    void CheckReturnsUnknown_AdminNamespaceAsNotAdmin(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/admin/connects"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void CheckReturnsAllowed_AdminNamespaceAsAdmin(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of("isAdmin()"));

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/admin/connects"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);
    }
    @Test
    void CheckReturnsAllowed_NamespaceAsAdmin(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of("isAdmin()"));
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/test/connects"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

    @Test
    void CheckReturnsAllowed(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of(RoleBinding.builder()
                        .metadata(ObjectMeta.builder().namespace("test")
                                .build())
                        .spec(RoleBinding.RoleBindingSpec.builder()
                                .role(RoleBinding.Role.builder()
                                        .resourceTypes(List.of(RoleBinding.ResourceType.connects))
                                        .verbs(List.of(RoleBinding.Verb.GET))
                                        .build())
                                .subject(RoleBinding.Subject.builder().subjectName("group1")
                                        .build())
                                .build())
                        .build()));
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/test/connects"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

  @Test
    void CheckReturnsUnknown_SubResource(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));
      Mockito.when(roleBindingRepository.findAllForGroups(groups))
              .thenReturn(List.of(RoleBinding.builder()
                      .metadata(ObjectMeta.builder().namespace("test")
                              .build())
                      .spec(RoleBinding.RoleBindingSpec.builder()
                              .role(RoleBinding.Role.builder()
                                      .resourceTypes(List.of(RoleBinding.ResourceType.connects))
                                      .verbs(List.of(RoleBinding.Verb.GET))
                                      .build())
                              .subject(RoleBinding.Subject.builder().subjectName("group1")
                                      .build())
                              .build())
                      .build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.check(HttpRequest.GET("/api/namespaces/test/connects/name/restart"),null, claims);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void ComputeRoles_NoAdmin(){
        resourceBasedSecurityRule.setAdminGroup("admin-group");
        List<String> actual = resourceBasedSecurityRule.computeRolesFromGroups(List.of("not-admin"));

        Assertions.assertIterableEquals(List.of(), actual);
    }

    @Test
    void ComputeRoles_Admin(){
        resourceBasedSecurityRule.setAdminGroup("admin-group");
        List<String> actual = resourceBasedSecurityRule.computeRolesFromGroups(List.of("admin-group"));

        Assertions.assertIterableEquals(List.of(ResourceBasedSecurityRule.IS_ADMIN), actual);
    }

}
