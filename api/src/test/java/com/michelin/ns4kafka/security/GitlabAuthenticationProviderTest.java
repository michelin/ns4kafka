package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.security.gitlab.GitlabAuthenticationProvider;
import com.michelin.ns4kafka.security.gitlab.GitlabAuthenticationService;
import com.michelin.ns4kafka.services.RoleBindingService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.*;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;

import java.util.List;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GitlabAuthenticationProviderTest {
    /**
     * The mocked GitLab authentication service
     */
    @Mock
    GitlabAuthenticationService gitlabAuthenticationService;

    /**
     * The mocked resource security service
     */
    @Mock
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    /**
     * The role binding service
     */
    @Mock
    RoleBindingService roleBindingService;

    /**
     * The NS4Kafka security config service
     */
    @Mock
    SecurityConfig securityConfig;

    /**
     * The mocked Gitlab authentication provider
     */
    @InjectMocks
    GitlabAuthenticationProvider gitlabAuthenticationProvider;

    /**
     * Assert the user authentication is successful
     */
    @Test
    void authenticationSuccess() {
        AuthenticationRequest<String, String> authenticationRequest = new UsernamePasswordCredentials("username","53cu23d_70k3n");

        List<String> groups = List.of("group-1","group-2");

        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace-rb1")
                        .cluster("local")
                        .build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group-1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret()))
                .thenReturn(Maybe.just("email"));
        when(gitlabAuthenticationService.findAllGroups(authenticationRequest.getSecret()))
                .thenReturn(Flowable.fromIterable(groups));
        when(roleBindingService.listByGroups(groups))
                .thenReturn(List.of(roleBinding));
        when(resourceBasedSecurityRule.computeRolesFromGroups(groups))
                .thenReturn(List.of());

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber<>();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = gitlabAuthenticationProvider.authenticate(null, authenticationRequest);
        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);

        AuthenticationResponse actual = subscriber.values().get(0);
        Assertions.assertTrue(actual.isAuthenticated());
        Assertions.assertTrue(actual.getUserDetails().isPresent());

        UserDetails actualUserDetails = actual.getUserDetails().get();
        Assertions.assertEquals("email", actualUserDetails.getUsername());
        Assertions.assertIterableEquals(groups, (List<String>)actualUserDetails.getAttributes("roles","username").get( "groups"));
        Assertions.assertIterableEquals(List.of(), actualUserDetails.getRoles(),"User has no custom roles");

    }

    /**
     * Assert the admin authentication is successful
     */
    @Test
    void authenticationSuccessAdmin() {
        AuthenticationRequest<String, String> authenticationRequest = new UsernamePasswordCredentials("admin","53cu23d_70k3n");

        List<String> groups = List.of("group-1","group-2","group-admin");
        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret()))
                .thenReturn(Maybe.just("email"));
        when(gitlabAuthenticationService.findAllGroups(authenticationRequest.getSecret()))
                .thenReturn(Flowable.fromIterable(groups));
        when(roleBindingService.listByGroups(groups))
                .thenReturn(List.of());
        when(securityConfig.getAdminGroup())
                .thenReturn("group-admin");
        when(resourceBasedSecurityRule.computeRolesFromGroups(groups))
                .thenReturn(List.of(ResourceBasedSecurityRule.IS_ADMIN));

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber<>();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertComplete();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);

        AuthenticationResponse actual = subscriber.values().get(0);
        Assertions.assertTrue(actual.isAuthenticated());
        Assertions.assertTrue(actual.getUserDetails().isPresent());

        UserDetails actualUserDetails = actual.getUserDetails().get();
        Assertions.assertEquals("email", actualUserDetails.getUsername());
        Assertions.assertIterableEquals(groups, (List<String>)actualUserDetails.getAttributes("roles","username").get("groups"));
        Assertions.assertIterableEquals(List.of(ResourceBasedSecurityRule.IS_ADMIN), actualUserDetails.getRoles(),"User has custom roles");
    }

    /**
     * Assert the authentication fails when GitLab responds HTTP 403
     */
    @Test
    void authenticationFailure() {
        AuthenticationRequest<String, String> authenticationRequest = new UsernamePasswordCredentials("admin","f4k3_70k3n");

        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret()))
                .thenThrow(new HttpClientResponseException("403 Unauthorized", HttpResponse.unauthorized()));

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber<>();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(AuthenticationException.class);
        subscriber.assertValueCount(0);
    }

    /**
     * Assert the authentication fails when GitLab responds HTTP 403
     */
    @Test
    void authenticationFailureGroupsNotFound() {
        AuthenticationRequest<String, String> authenticationRequest = new UsernamePasswordCredentials("admin","f4k3_70k3n");

        List<String> groups = List.of("group-1","group-2");
        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret()))
                .thenReturn(Maybe.just("email"));
        when(gitlabAuthenticationService.findAllGroups(authenticationRequest.getSecret()))
                .thenReturn(Flowable.fromIterable(groups));
        when(roleBindingService.listByGroups(groups))
                .thenReturn(List.of());
        when(securityConfig.getAdminGroup())
                .thenReturn("group-admin");

        TestSubscriber<AuthenticationResponse> subscriber = new TestSubscriber<>();
        Publisher<AuthenticationResponse> authenticationResponsePublisher = gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        authenticationResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(AuthenticationException.class);
        subscriber.assertValueCount(0);
    }
}
