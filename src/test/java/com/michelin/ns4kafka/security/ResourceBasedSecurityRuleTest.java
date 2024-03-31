package com.michelin.ns4kafka.security;

import static com.michelin.ns4kafka.models.RoleBinding.Verb.GET;
import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLES;
import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLE_BINDINGS;
import static com.nimbusds.jwt.JWTClaimNames.SUBJECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.properties.SecurityProperties;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.security.auth.AuthenticationRoleBinding;
import com.michelin.ns4kafka.utils.exceptions.ForbiddenNamespaceException;
import com.michelin.ns4kafka.utils.exceptions.UnknownNamespaceException;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRuleResult;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResourceBasedSecurityRuleTest {
    private static final String NAMESPACE = "namespace";
    private static final String VERBS = "verbs";
    private static final String RESOURCE_TYPES = "resourceTypes";

    @Mock
    NamespaceRepository namespaceRepository;

    @Mock
    SecurityProperties securityProperties;

    @InjectMocks
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @Test
    void shouldReturnUnknownWhenUnauthenticated() {
        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/anything"), null);
        assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void shouldReturnUnknownWhenMissingClaims() {
        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of());
        Authentication auth = Authentication.build("user", claims);

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/anything"), auth);
        assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @ParameterizedTest
    @CsvSource({"/non-namespaced/resource", "/api/namespaces"})
    void shouldReturnUnknownWhenInvalidResource(String path) {
        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, List.of());
        Authentication auth = Authentication.build("user", claims);

        SecurityRuleResult actual =
            resourceBasedSecurityRule.checkSecurity(HttpRequest.GET(path), auth);
        assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void shouldReturnUnknownNamespace() {
        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, List.of());
        Authentication auth = Authentication.build("user", claims);

        when(namespaceRepository.findByName("test"))
            .thenReturn(Optional.empty());

        HttpRequest<?> request = HttpRequest.GET("/api/namespaces/test/connectors");

        UnknownNamespaceException exception = assertThrows(UnknownNamespaceException.class,
            () -> resourceBasedSecurityRule.checkSecurity(request, auth));

        assertEquals("Accessing unknown namespace \"test\"", exception.getMessage());
    }

    @ParameterizedTest
    @CsvSource({"name$space", "*namespace*"})
    void shouldReturnUnknownWhenWrongNamespaceName(String namespace) {
        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, List.of());
        Authentication auth = Authentication.build("user", claims);

        SecurityRuleResult actual =
            resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/" + namespace + "/topics"), auth);

        assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void shouldReturnUnknownNamespaceAsAdmin() {
        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of("isAdmin()"), ROLE_BINDINGS, List.of());
        Authentication auth = Authentication.build("user", List.of("isAdmin()"), claims);

        when(namespaceRepository.findByName("admin"))
            .thenReturn(Optional.empty());

        HttpRequest<?> request = HttpRequest.GET("/api/namespaces/admin/connectors");

        UnknownNamespaceException exception = assertThrows(UnknownNamespaceException.class,
            () -> resourceBasedSecurityRule.checkSecurity(request, auth));

        assertEquals("Accessing unknown namespace \"admin\"", exception.getMessage());
    }

    @Test
    void checkReturnsAllowedNamespaceAsAdmin() {
        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of("isAdmin()"), ROLE_BINDINGS, List.of());
        Authentication auth = Authentication.build("user", List.of("isAdmin()"), claims);

        when(namespaceRepository.findByName("test"))
            .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual =
            resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors"), auth);
        assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

    @ParameterizedTest
    @CsvSource({"connectors,/api/namespaces/test/connectors",
        "role-bindings,/api/namespaces/test/role-bindings",
        "topics,/api/namespaces/test/topics/topic.with.dots"})
    void shouldReturnAllowedWhenHyphenAndDotResourcesAndHandleRoleBindingsType(String resourceType, String path) {
        List<Map<String, ?>> jwtRoleBindings = List.of(
            Map.of(NAMESPACE, "test",
                VERBS, List.of(GET),
                RESOURCE_TYPES, List.of(resourceType)));

        Map<String, Object> jwtClaims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, jwtRoleBindings);
        Authentication jwtAuth = Authentication.build("user", jwtClaims);

        when(namespaceRepository.findByName("test"))
            .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult jwtActual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET(path), jwtAuth);
        assertEquals(SecurityRuleResult.ALLOWED, jwtActual);

        List<AuthenticationRoleBinding> basicAuthRoleBindings = List.of(
            AuthenticationRoleBinding.builder()
                .namespace("test")
                .verbs(List.of(GET))
                .resourceTypes(List.of(resourceType))
                .build());

        Map<String, Object> basicAuthClaims =
            Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, basicAuthRoleBindings);
        Authentication basicAuth = Authentication.build("user", basicAuthClaims);

        when(namespaceRepository.findByName("test"))
            .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult basicAuthActual = resourceBasedSecurityRule.checkSecurity(HttpRequest.GET(path), basicAuth);
        assertEquals(SecurityRuleResult.ALLOWED, basicAuthActual);
    }

    @Test
    void shouldReturnAllowedWhenSubResource() {
        List<Map<String, ?>> jwtRoleBindings = List.of(
            Map.of(NAMESPACE, "test",
                VERBS, List.of(GET),
                RESOURCE_TYPES, List.of("connectors/restart", "topics/delete-records")));

        Map<String, Object> jwtClaims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, jwtRoleBindings);
        Authentication jwtAuth = Authentication.build("user", jwtClaims);

        when(namespaceRepository.findByName("test"))
            .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule
            .checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors/name/restart"), jwtAuth);

        assertEquals(SecurityRuleResult.ALLOWED, actual);

        actual = resourceBasedSecurityRule
            .checkSecurity(HttpRequest.GET("/api/namespaces/test/topics/name/delete-records"), jwtAuth);

        assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

    @ParameterizedTest
    @CsvSource({"namespace", "name-space", "name.space", "_name_space_", "namespace123"})
    void shouldReturnAllowedWhenSpecialNamespaceName(String namespace) {
        List<Map<String, ?>> roleBindings = List.of(
            Map.of(NAMESPACE, namespace,
                VERBS, List.of(GET),
                RESOURCE_TYPES, List.of("topics")));

        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, roleBindings);
        Authentication auth = Authentication.build("user", claims);

        when(namespaceRepository.findByName(namespace))
            .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual =
            resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/" + namespace + "/topics"), auth);
        assertEquals(SecurityRuleResult.ALLOWED, actual);
    }

    @Test
    void shouldReturnForbiddenNamespaceWhenNoRoleBinding() {
        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, List.of());
        Authentication auth = Authentication.build("user", claims);

        when(namespaceRepository.findByName("test"))
            .thenReturn(Optional.of(Namespace.builder().build()));

        HttpRequest<?> request = HttpRequest.GET("/api/namespaces/test/connectors");

        ForbiddenNamespaceException exception = assertThrows(ForbiddenNamespaceException.class,
            () -> resourceBasedSecurityRule.checkSecurity(request, auth));

        assertEquals("Accessing forbidden namespace \"test\"", exception.getMessage());
    }

    @Test
    void shouldReturnForbiddenNamespaceWhenNoRoleBindingMatchingRequestedNamespace() {
        List<Map<String, ?>> roleBindings = List.of(
            Map.of(NAMESPACE, "namespace",
                VERBS, List.of(GET),
                RESOURCE_TYPES, List.of("connectors")));

        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, roleBindings);
        Authentication auth = Authentication.build("user", claims);

        when(namespaceRepository.findByName("forbiddenNamespace"))
            .thenReturn(Optional.of(Namespace.builder().build()));

        HttpRequest<?> request = HttpRequest.GET("/api/namespaces/forbiddenNamespace/connectors");

        ForbiddenNamespaceException exception = assertThrows(ForbiddenNamespaceException.class,
            () -> resourceBasedSecurityRule.checkSecurity(request, auth));

        assertEquals("Accessing forbidden namespace \"forbiddenNamespace\"", exception.getMessage());
    }

    @Test
    void checkReturnsUnknownSubResource() {
        List<Map<String, ?>> roleBindings = List.of(
            Map.of(NAMESPACE, "test",
                VERBS, List.of(GET),
                RESOURCE_TYPES, List.of("connectors")));

        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, roleBindings);
        Authentication auth = Authentication.build("user", claims);

        when(namespaceRepository.findByName("test"))
            .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual =
            resourceBasedSecurityRule.checkSecurity(HttpRequest.GET("/api/namespaces/test/connectors/name/restart"),
                auth);
        assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void checkReturnsUnknownSubResourceWithDot() {
        List<Map<String, ?>> roleBindings = List.of(
            Map.of(NAMESPACE, "test",
                VERBS, List.of(GET),
                RESOURCE_TYPES, List.of("connectors")));

        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of(), ROLE_BINDINGS, roleBindings);
        Authentication auth = Authentication.build("user", claims);

        when(namespaceRepository.findByName("test"))
            .thenReturn(Optional.of(Namespace.builder().build()));

        SecurityRuleResult actual = resourceBasedSecurityRule.checkSecurity(
            HttpRequest.GET("/api/namespaces/test/connectors/name.with.dots/restart"), auth);
        assertEquals(SecurityRuleResult.UNKNOWN, actual);
    }

    @Test
    void computeRolesNoAdmin() {
        when(securityProperties.getAdminGroup())
            .thenReturn("admin-group");
        List<String> actual = resourceBasedSecurityRule.computeRolesFromGroups(List.of("not-admin"));

        Assertions.assertIterableEquals(List.of(), actual);
    }

    @Test
    void computeRolesAdmin() {
        when(securityProperties.getAdminGroup())
            .thenReturn("admin-group");
        List<String> actual = resourceBasedSecurityRule.computeRolesFromGroups(List.of("admin-group"));

        Assertions.assertIterableEquals(List.of(ResourceBasedSecurityRule.IS_ADMIN), actual);
    }
}
