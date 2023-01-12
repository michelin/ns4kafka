package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
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
class ResourceBasedSecurityRuleTest {
    @Mock
    NamespaceRepository namespaceRepository;

    @Mock
    RoleBindingRepository roleBindingRepository;

    @Mock
    SecurityConfig securityConfig;

    @InjectMocks
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @Test
    void checkReturnsUnknownUnauthenticated(){
        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/anything"),null);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void checkReturnsUnknownMissingClaims(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups);
        Authentication auth = Authentication.build("user", claims);

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/anything"), auth);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void checkReturnsUnknownInvalidResource(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/non-namespaced/resource"), auth);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void checkReturnsUnknownNoRoleBinding(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of());

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors"), auth);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void checkReturnsUnknownInvalidNamespace(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.empty());

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors"), auth);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void checkReturnsUnknownAdminNamespaceAsNotAdmin(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/admin/connectors"), auth);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void checkReturnsUnknownInvalidNamespaceAsAdmin(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of("isAdmin()"));
        Authentication auth = Authentication.build("user", List.of("isAdmin()"), claims);

        Mockito.when(namespaceRepository.findByName("admin"))
                .thenReturn(Optional.empty());

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/admin/connectors"), auth);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void checkReturnsAllowedNamespaceAsAdmin(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of("isAdmin()"));
        Authentication auth = Authentication.build("user", List.of("isAdmin()"), claims);

        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors"), auth);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

    @Test
    void checkReturnsAllowed(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of(RoleBinding.builder()
                        .metadata(ObjectMeta.builder().namespace("test")
                                .build())
                        .spec(RoleBinding.RoleBindingSpec.builder()
                                .role(RoleBinding.Role.builder()
                                        .resourceTypes(List.of("connectors"))
                                        .verbs(List.of(RoleBinding.Verb.GET))
                                        .build())
                                .subject(RoleBinding.Subject.builder().subjectName("group1")
                                        .build())
                                .build())
                        .build()));
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors"), auth);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

    @Test
    void CheckReturnsAllowedSubresource() {
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of(RoleBinding.builder()
                        .metadata(ObjectMeta.builder().namespace("test")
                                .build())
                        .spec(RoleBinding.RoleBindingSpec.builder()
                                .role(RoleBinding.Role.builder()
                                        .resourceTypes(List.of("connectors/restart","topics/delete-records"))
                                        .verbs(List.of(RoleBinding.Verb.GET))
                                        .build())
                                .subject(RoleBinding.Subject.builder().subjectName("group1")
                                        .build())
                                .build())
                        .build()));
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors/name/restart"), auth);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);

        actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/topics/name/delete-records"), auth);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

    @Test
    void CheckReturnsAllowedResourceWithHyphen() {
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of(RoleBinding.builder()
                        .metadata(ObjectMeta.builder().namespace("test")
                                .build())
                        .spec(RoleBinding.RoleBindingSpec.builder()
                                .role(RoleBinding.Role.builder()
                                        .resourceTypes(List.of("role-bindings"))
                                        .verbs(List.of(RoleBinding.Verb.GET))
                                        .build())
                                .subject(RoleBinding.Subject.builder().subjectName("group1")
                                        .build())
                                .build())
                        .build()));
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/role-bindings"), auth);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

    @Test
    void CheckReturnsAllowedResourceNameWithDot() {
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of(RoleBinding.builder()
                        .metadata(ObjectMeta.builder().namespace("test")
                                .build())
                        .spec(RoleBinding.RoleBindingSpec.builder()
                                .role(RoleBinding.Role.builder()
                                        .resourceTypes(List.of("topics"))
                                        .verbs(List.of(RoleBinding.Verb.GET))
                                        .build())
                                .subject(RoleBinding.Subject.builder().subjectName("group1")
                                        .build())
                                .build())
                        .build()));
        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/topics/topic.with.dots"), auth);
        Assertions.assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

    @Test
    void CheckReturnsUnknownSubResource(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(roleBindingRepository.findAllForGroups(groups))
              .thenReturn(List.of(RoleBinding.builder()
                      .metadata(ObjectMeta.builder().namespace("test")
                              .build())
                      .spec(RoleBinding.RoleBindingSpec.builder()
                              .role(RoleBinding.Role.builder()
                                      .resourceTypes(List.of("connectors"))
                                      .verbs(List.of(RoleBinding.Verb.GET))
                                      .build())
                              .subject(RoleBinding.Subject.builder().subjectName("group1")
                                      .build())
                              .build())
                      .build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors/name/restart"), auth);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void CheckReturnsUnknownSubResourceWithDot(){
        List<String> groups = List.of("group1");
        Map<String,Object> claims = Map.of("sub","user", "groups", groups, "roles", List.of());
        Authentication auth = Authentication.build("user", claims);

        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(roleBindingRepository.findAllForGroups(groups))
                .thenReturn(List.of(RoleBinding.builder()
                        .metadata(ObjectMeta.builder().namespace("test")
                                .build())
                        .spec(RoleBinding.RoleBindingSpec.builder()
                                .role(RoleBinding.Role.builder()
                                        .resourceTypes(List.of("connectors"))
                                        .verbs(List.of(RoleBinding.Verb.GET))
                                        .build())
                                .subject(RoleBinding.Subject.builder().subjectName("group1")
                                        .build())
                                .build())
                        .build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors/name.with.dots/restart"), auth);
        Assertions.assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void ComputeRolesNoAdmin() {
        Mockito.when(securityConfig.getAdminGroup())
                .thenReturn("admin-group");
        List<String> actual = resourceBasedSecurityRule.computeRolesFromGroups(List.of("not-admin"));

        Assertions.assertIterableEquals(List.of(), actual);
    }

    @Test
    void ComputeRolesAdmin() {
        Mockito.when(securityConfig.getAdminGroup())
                .thenReturn("admin-group");
        List<String> actual = resourceBasedSecurityRule.computeRolesFromGroups(List.of("admin-group"));

        Assertions.assertIterableEquals(List.of(ResourceBasedSecurityRule.IS_ADMIN), actual);
    }
}
